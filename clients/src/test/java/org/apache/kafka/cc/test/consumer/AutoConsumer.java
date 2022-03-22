package org.apache.kafka.cc.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * User: chenchong
 * Date: 2018/11/9
 * description:  自动提交
 */

public class AutoConsumer {

	private static final String brokerUrl = "localhost:9092";
//private static final String brokerUrl = "172.18.1.0:9092";
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerUrl);
		props.put("group.id", "aa");
		// 自动提交 offset
		props.put("enable.auto.commit", true);
		// 自动提交间隔
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);




//		TopicPartition tp = new TopicPartition("demo2",0);
		/**
		 * 1. 清除 Fetcher 中的 缓存 旧的topic
		 * 2. 确保取消订阅的主题分期的偏移量已经提交了
		 * 3. 更新 metaData 和 SubscriptionState 的 topic 信息
		 */
//		consumer.assign(Arrays.asList(tp));


		/**
		 * 1. 更新 metaData 和 SubscriptionState 的 topic 信息
		 * 2. SubscriptionState 添加 ConsumerRebalanceListener 如果不为空话
		 */
		consumer.subscribe(Arrays.asList("logdata"));


//		Pattern p = Pattern.compile("test");
		/**
		 * 1. SubscriptionState 将 pattern 缓存起来
		 * 2. metaData 标记为更新所有的 topic 信息，metaData 等待更新
		 * 3. SubscriptionState 添加 ConsumerRebalanceListener 如果不为空话
		 */
//		consumer.subscribe(p);




 		while (true) {

		// kafka consumer 不是线程安全的，不支持多线程

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());


		}
	}
}
