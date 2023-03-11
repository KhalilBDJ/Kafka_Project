package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

public class CovidDataProducer {


    private static final String KAFKA_SERVERS = "localhost:9092"; // adresse du serveur kafka

    public static void main(String[] args) throws IOException {
        CovidDataProducer producer = new CovidDataProducer();
        producer.start();
    }

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                SendCovidData();
            } catch (Exception e) {
                System.err.println("Failed to send COVID data to Kafka: " + e.getMessage());
            }
        }, 0, 30, TimeUnit.MINUTES);
    }

    public void stop() {
        scheduler.shutdown();
    }

    private void SendCovidData() throws IOException {
        // configure producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AdminClient adminClient = AdminClient.create(props);

        TopicPartition topicPartition = new TopicPartition("Topic1", 0);
        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
        recordsToDelete.put(topicPartition, RecordsToDelete.beforeOffset(1L));

        DeleteRecordsResult deleteResult = adminClient.deleteRecords(recordsToDelete);
        try {
            deleteResult.all().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        // create kafka producer
        URL url = new URL("https://api.covid19api.com/summary");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }

        in.close();
        String jsonString = response.toString();
        String topicName = "Topic1";
        //deleteTopicData("testTopic", 0, 1000000, adminClient);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, jsonString);
        producer.send(record);

        producer.close();
    }

    public void deleteTopicData(String topicName, int partitionIndex, int beforeIndex, AdminClient kafkaAdminClient) {

    }




}
