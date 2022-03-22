package org.apache.kafka.cc.test.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final String brokerUrl = "localhost:9092";

    public static void main(String[] args) throws Exception {

        String topic = "demo6";

        // Load properties from disk.
        Properties props = new Properties();

        // Add additional properties.
        props.put("bootstrap.servers", brokerUrl);
        // 自动提交 offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        // WARNING: These settings are for CLI tools / examples. Don't use in production unless you want to reset the consumer app every reboot.
        props.put("group.id", "cc22");
        props.put("auto.offset.reset", "earliest");

        final org.apache.kafka.clients.consumer.Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
