package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class CommandProducerWriter {

    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "Topic2";

    public static void main(String[] args) throws IOException{

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter a command to send to Kafka: ");
            String input = scanner.nextLine();

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, input);

            producer.send(record);

            System.out.println("Sent command: " + input + " to Kafka topic: " + TOPIC_NAME);
        }

        //producer.close();
    }
}
