package org.apache.kafka.cc.test.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;

import java.util.*;

/**
 * 0.11 版本后提供的交互方式
 */
public class KafkaAdminClient {

    private static final AdminClient client;
    private static final String brokerUrl = "172.18.1.2:9092,172.18.1.3:9092,172.18.1.0:9092";

    private KafkaAdminClient(){}

    static {

        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        client = AdminClient.create(p);
    }

    public static Set<String> listTopics(){
        try {
            ListTopicsOptions options = new ListTopicsOptions();
//            options.listInternal(true); // includes internal topics such as __consumer_offsets
            ListTopicsResult result = client.listTopics(options);
            return result.names().get();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static boolean createTopic(NewTopic c){
        try{
            CreateTopicsResult result  = client.createTopics(Arrays.asList(c));
            result.all().get();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static boolean createTopics(Collection<NewTopic> c){
        try{
            CreateTopicsResult result  = client.createTopics(c);
            result.all().get();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static void describeTopic(String topic){
        try{
            DescribeTopicsResult result = client.describeTopics(Arrays.asList(topic));
            Map<String,TopicDescription> map =  result.all().get();
            for(Map.Entry entry : map.entrySet()){
                System.out.println(entry.getKey() +" >>>>> " + entry.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void describeTopics(Collection c){
        try{
            DescribeTopicsResult result = client.describeTopics(c);
            Map<String,TopicDescription> map =  result.all().get();
            for(Map.Entry entry : map.entrySet()){
                System.out.println(entry.getKey() +" >>>>> " + entry.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void delTopics(Collection c){
        try{
            client.deleteTopics(c);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void delAllTopics(){
        try{
            Set<String> set = KafkaAdminClient.listTopics();
            KafkaAdminClient.delTopics(set);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
// hello_cc_2
//topics: hello_cc_3
    public static void main(String[] args){

      /*   Set<String> set = KafkaAdminClient.listTopics();
         KafkaAdminClient.delTopics(set);*/

        NewTopic topic = new NewTopic("replica",3,(short)3);
        KafkaAdminClient.listTopics().stream().forEach(it->{
            it.toString();
        });



//        KafkaAdminClient.delAllTopics();
//    NewTopic topic = new NewTopic("demo",3, (short) 1);
         KafkaAdminClient.createTopic(topic);
     /*  KafkaAdminClient.delAllTopics();
        KafkaAdminClient.listTopics().stream().forEach(it->{
            System.out.println(it.toString());
        });*/
//        KafkaAdminClient.describeTopic("demo");
    }
}
