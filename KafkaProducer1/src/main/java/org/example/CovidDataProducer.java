package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class CovidDataProducer {

    private static final String KAFKA_SERVERS = "localhost:9092"; // adresse du serveur kafka
    private static final String TOPIC_NAME = "Topic1";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(CovidDataProducer.class, args);

        // Créer un bean KafkaAdmin pour pouvoir créer des topics
        KafkaAdmin admin = (KafkaAdmin) context.getBean("kafkaAdmin");
        AdminClient client = AdminClient.create(admin.getConfigurationProperties());

        // Vérifier si le topic existe
        if (!client.listTopics().names().get().contains(TOPIC_NAME)) {
            // Créer le topic avec une partition et un replication factor de 1
            NewTopic topic = new NewTopic(TOPIC_NAME, 1, (short) 1);
            client.createTopics(Collections.singletonList(topic));
        }
    }

    // Envoyer les données COVID toutes les 30 minutes
    @Scheduled(fixedDelay = 30 * 60 * 1000)
    public void sendCovidData() throws IOException {
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
        String topicName = TOPIC_NAME;

        // Envoyer le message
        ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, jsonString);
        kafkaTemplate.send(record);
    }

    // Définir les propriétés du producteur
    @Bean
    public Properties producerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }
}