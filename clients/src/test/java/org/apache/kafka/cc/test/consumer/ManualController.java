package org.apache.kafka.cc.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: chenchong
 * Date: 2018/11/9
 * description:  手动提交
 */
public class ManualController {

	public static void main(String[] args){
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("group.id","test");
		// 关闭自动提交
		props.put("enable.auto.commit","false");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("foo","bar"));
		final int minBatchSize = 200;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while(true)
		{
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
			if (buffer.size() >= minBatchSize) {
				// 这一步骤可能会失败，因此采用 手动提交 offset 的方式
				//
				insertIntoDb(buffer);
				// 异步提交 offset
				consumer.commitSync();
				buffer.clear();
			}
		}
	}

	private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer){}

}
