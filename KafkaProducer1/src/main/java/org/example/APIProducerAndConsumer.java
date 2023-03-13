package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class APIProducerAndConsumer {


    private final static String TOPIC_NAME2 = "Topic2";
    private final static String TOPIC_NAME3 = "Topic3";
    private final static String GROUP_ID = "group1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private String currentRecord;
    private String response;
    public static void main(String[] args){
        APIProducerAndConsumer apiProducerAndConsumer = new APIProducerAndConsumer();
        apiProducerAndConsumer.StartConsumer();
    }

    private void StartConsumer(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(TOPIC_NAME2)) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(partitions);

        // Seek to the end of each partition
        consumer.seekToEnd(partitions);
        System.out.println("Listening to topic: " + TOPIC_NAME2);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value()); // A MODIFIER POUR EFFECTUER DES COMMANDES
                currentRecord = record.value();
                kafkaInterface();
            }
        }
    }

    private void kafkaInterface(){
        if (currentRecord != null){
            switch (currentRecord){
                case "Get_global_values":
                    response = "Voici les global values";
                    System.out.println(response);
                    StartProducer();
                    break;
                case "Get_country_values Algerie":
                    response="DZ Power";
                    System.out.println(response);
                    StartProducer();
                    break;
                case "Get_confirmed_avg":
                    response = "La moyenne des confirmés est";
                    System.out.println(response);
                    StartProducer();
                    break;
                case "Get_deaths_avg":
                    response = "La moyenne des morts est";
                    System.out.println(response);
                    StartProducer();
                    break;
                case "Get_countries_deaths_percent":
                    response = "Le pourcentage de mort pour le pays est";
                    System.out.println(response);
                    StartProducer();
                    break;
            }
        }
    }

    private void StartProducer(){

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        Producer<String, String> producer = new KafkaProducer<>(props);
        if (response != null){
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME3, response);
            producer.send(record);
            System.out.println("Sent command: " + response + " to Kafka topic: " + TOPIC_NAME3);
        }
        producer.close();
    }
}
