/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Optional

import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicaQuota,
                           leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false) {

  private val replicaId = brokerConfig.brokerId
  private val logContext = new LogContext(s"[ReplicaFetcher replicaId=$replicaId, leaderId=${sourceBroker.id}, " +
    s"fetcherId=$fetcherId] ")
  this.logIdent = logContext.logPrefix

  // ???????????? NIO ?????????
  private val leaderEndpoint = leaderEndpointBlockingSend.getOrElse(
    new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-$replicaId-fetcher-$fetcherId", logContext))

  // Visible for testing
  private[server] val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 9
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 8
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_1_1_IV0) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0

  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private val brokerSupportsLeaderEpochRequest = brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV2
  private val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.getReplicaOrException(topicPartition).epochs.map(_.latestEpoch)
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.getReplicaOrException(topicPartition).logEndOffset.messageOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    val replica = replicaMgr.getReplicaOrException(topicPartition)
    replica.epochs.flatMap { epochCache =>
      val (foundEpoch, foundOffset) = epochCache.endOffsetFor(epoch)
      if (foundOffset == UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(foundOffset, foundEpoch))
    }
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      leaderEndpoint.close()
    }
    justShutdown
  }

  // process fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: PD): Option[LogAppendInfo] = {
    val replica = replicaMgr.getReplicaOrException(topicPartition)
    val partition = replicaMgr.getPartition(topicPartition).get
    val records = toMemoryRecords(partitionData.records)

    maybeWarnIfOversizedRecords(records, topicPartition)


    if (fetchOffset != replica.logEndOffset.messageOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, replica.logEndOffset.messageOffset))

    if (isTraceEnabled)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(replica.logEndOffset.messageOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)

    if (isTraceEnabled)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(replica.logEndOffset.messageOffset, records.sizeInBytes, topicPartition))

    // ?????? HW ??? Fetch ??? HW ??????
    val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
    val leaderLogStartOffset = partitionData.logStartOffset
    // for the follower replica, we do not need to keep
    // its segment base offset the physical position,
    // these values will be computed upon making the leader
    replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)

    // ?????? startOffset
    replica.maybeIncrementLogStartOffset(leaderLogStartOffset)
    if (isTraceEnabled)
      trace(s"Follower set replica high watermark for partition $topicPartition to $followerHighWatermark")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)
    replicaMgr.brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  override protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, PD)] = {
    try {
      val clientResponse = leaderEndpoint.sendRequest(fetchRequest)
      val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse[Records]]
      if (!fetchSessionHandler.handleResponse(fetchResponse)) {
        Nil
      } else {
        fetchResponse.responseData.asScala.toSeq
      }
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition): Long = {
    fetchOffsetFromLeader(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP)
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition): Long = {
    fetchOffsetFromLeader(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP)
  }

  private def fetchOffsetFromLeader(topicPartition: TopicPartition, earliestOrLatest: Long): Long = {
    val requestPartitionData = new ListOffsetRequest.PartitionData(earliestOrLatest, Optional.empty[Integer]())
    val requestPartitions = Map(topicPartition -> requestPartitionData)
    val requestBuilder = ListOffsetRequest.Builder.forReplica(listOffsetRequestVersion, replicaId)
      .setTargetTimes(requestPartitions.asJava)

    val clientResponse = leaderEndpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]

    val responsePartitionData = response.responseData.get(topicPartition)
    responsePartitionData.error match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          responsePartitionData.offset
        else
          responsePartitionData.offsets.get(0)
      case error => throw error.exception
    }
  }

  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[FetchRequest.Builder]] = {
    val partitionsWithError = mutable.Set[TopicPartition]()

    val builder = fetchSessionHandler.newBuilder()
    partitionMap.foreach { case (topicPartition, partitionFetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      if (partitionFetchState.isReadyForFetch && !shouldFollowerThrottle(quota, topicPartition)) {
        try {
          // ????????????????????? offset
          val logStartOffset = replicaMgr.getReplicaOrException(topicPartition).logStartOffset
          // fetchOffset ?????? ?????? follower ??? leo
          builder.add(topicPartition, new FetchRequest.PartitionData(
            partitionFetchState.fetchOffset, logStartOffset, fetchSize, Optional.empty()))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }

    val fetchData = builder.build()
    val fetchRequestOpt = if (fetchData.sessionPartitions.isEmpty && fetchData.toForget.isEmpty) {
      None
    } else {
      val requestBuilder = FetchRequest.Builder
        .forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .toForget(fetchData.toForget)
        .metadata(fetchData.metadata)
      Some(requestBuilder)
    }

    ResultWithPartitions(fetchRequestOpt, partitionsWithError)
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val replica = replicaMgr.getReplicaOrException(tp)
    val partition = replicaMgr.getPartition(tp).get
    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    if (offsetTruncationState.offset < replica.highWatermark.messageOffset)
      warn(s"Truncating $tp to offset ${offsetTruncationState.offset} below high watermark " +
        s"${replica.highWatermark.messageOffset}")

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp, offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartition(topicPartition).get
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  // ?????? LIST_OFFSETS ????????? ????????? leader epoch ??? lastOffset
  override def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
    var result: Map[TopicPartition, EpochEndOffset] = null
    if (brokerSupportsLeaderEpochRequest) {
      // skip request for partitions without epoch, as their topic log message format doesn't support epochs
      val (partitionsWithEpoch, partitionsWithoutEpoch) = partitions.partition { case (_, epoch) => epoch != UNDEFINED_EPOCH }
      val resultWithoutEpoch = partitionsWithoutEpoch.map { case (tp, _) => (tp, new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)) }

      if (partitionsWithEpoch.isEmpty) {
        debug("Skipping leaderEpoch request since all partitions do not have an epoch")
        return resultWithoutEpoch
      }

      val partitionsAsJava = partitions.map { case (tp, epoch) => tp ->
        new OffsetsForLeaderEpochRequest.PartitionData(Optional.empty(), epoch.asInstanceOf[Integer])
      }.toMap.asJava
      val epochRequest = new OffsetsForLeaderEpochRequest.Builder(offsetForLeaderEpochRequestVersion, partitionsAsJava)
      try {
        val response = leaderEndpoint.sendRequest(epochRequest)

        result = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse].responses.asScala ++ resultWithoutEpoch
        debug(s"Receive leaderEpoch response $result; Skipped request for partitions ${partitionsWithoutEpoch.keys}")
      } catch {
        case t: Throwable =>
          warn(s"Error when sending leader epoch request for $partitions", t)

          // if we get any unexpected exception, mark all partitions with an error
          result = partitions.map { case (tp, _) =>
            tp -> new EpochEndOffset(Errors.forException(t), UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          }
      }
    } else {
      // just generate a response with no error but UNDEFINED_OFFSET so that we can fall back to truncating using
      // high watermark in maybeTruncate()
      result = partitions.map { case (tp, _) =>
        tp -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
    result
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, topicPartition: TopicPartition): Boolean = {
    val isReplicaInSync = fetcherLagStats.isReplicaInSync(topicPartition)
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

}
