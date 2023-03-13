package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CommandConsumer extends Thread{
    private static final String BOOSTSRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME3 = "Topic3";
    private final static String GROUP_ID = "group1";


    public static void main(String[] args) throws IOException{
        CommandConsumer commandConsumer = new CommandConsumer();
        commandConsumer.StartConsumer();
    }
    private void StartConsumer(){

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTSRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(TOPIC_NAME3)) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(partitions);

        // Seek to the end of each partition
        consumer.seekToEnd(partitions);
        System.out.println("Listening to topic: " + TOPIC_NAME3);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value()); // A MODIFIER POUR EFFECTUER DES COMMANDES
            }
        }
    }
}
