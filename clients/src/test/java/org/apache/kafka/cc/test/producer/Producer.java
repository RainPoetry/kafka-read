package org.apache.kafka.cc.test.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * 1. 线程安全
 * 2. 一个 KafkaProducer 实例多线程发送数据 比 多个 KafkaProducer 实例发送数据要快
 * 3. 使用完毕后要 close ,释放资源
 * 4. acks：判断数据发送成功的方式
 *      0 : server 端直接返回 response
 *      1: 等待leader 成功写入到本地 Log
 *      -1 / all ： 等待 leader 和 isr 副本成功写入到本地 log
 * 5. retries： 重试，也许会导致消息重复
 * 6. batch.size : 数据每次发送的大小
 * 7. linger.ms： 请求发送间隔时间
 *         即使 linger.ms=0 也会发生批处理，比如当两条记录几乎同时到达时
 *  8. buffer.memory ： 缓冲区大小
 *  9. max.block.ms ： 当 buffer.memory  空间不足以分配内存，最大等待时间，超时跑异常
 *  10. enable.idempotence:
 *          retries： 默认最大值
 *          acks：all
 *          idempotent producer  ： exactly once
 *  11. transactional.id :
 *      replication.factor: 至少为3
 *      min.insync.replicas = 2
 *      transactional producer: 同时向多个 partition、topics 发送数据
 */
public class Producer {
    private static final String brokerUrl = "localhost:9092";
//    private static final String brokerUrl = "172.18.1.3:9092,172.18.1.0:9092";

    public static void main(String[] args) throws IOException {


        long events = 100;
        String topic = "demo";

        // Load properties from disk.
        Properties props = new Properties();

        // Add additional properties.
        props.put("bootstrap.servers", brokerUrl);
        props.put("acks", "all");
        props.put("retries", 200);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
//        props.put("compression.type", "lz4");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", StringSerializer.class);
//        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,0);

        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
            // Produce sample data.
            Random rnd = new Random();
           /* for (long nEvents = 0; nEvents < events; nEvents++) {
                long runtime = new Date().getTime();
                String site = "www.example.com";
                String ip = "192.168.2." + rnd.nextInt(255);
                String record = String.format("%s,%s,%s", runtime, site, ip);
                producer.send(new ProducerRecord<>(topic, ip, record));
            }*/
            producer.initTransactions();
//           Thread.sleep(10000);
                for (int i = 0; i < 5; i++) {
                    producer.send(new ProducerRecord<>(topic, "key-" + i, "value-"+i), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
//                            System.out.println("callback: " + metadata.toString());
                        }
                    });
                }
//                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, 2,"key-" + i + 9, "value:" + i + 9));

//               producer.send(new ProducerRecord<>("demo", 2,"key-" + i + 9, "value:" + i + 9));


                // 关闭线程资源
//                producer.close();
            } catch(Exception e){
                e.printStackTrace();
            }finally {

        }

    }
}
