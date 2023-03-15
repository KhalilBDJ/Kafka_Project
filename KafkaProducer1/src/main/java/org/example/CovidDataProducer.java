package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
@Import(KafkaConfiguration.class) // Importez la configuration Kafka ici
@Component
public class CovidDataProducer {

    private static final String KAFKA_SERVERS = "localhost:9092"; // adresse du serveur kafka
    private static final String TOPIC_NAME = "Topic1";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Async
    public void Start(ConfigurableApplicationContext context) throws ExecutionException, InterruptedException {
        //ConfigurableApplicationContext context = SpringApplication.run(CovidDataProducer.class);

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
}
