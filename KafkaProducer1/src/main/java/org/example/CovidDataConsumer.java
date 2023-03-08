package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;

public class CovidDataConsumer {

    private static final String TOPIC_NAME = "testTopic";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/kafkadb";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "1797";

    public static void main(String[] args) throws JSONException {

        // Configurer les propriétés du consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Créer le consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Abonner le consumer au topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        // Boucle infinie de consommation des messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();
                JSONObject jsonObject = new JSONObject(jsonString);

                // Récupérer les données de l'objet JSON et les stocker dans la base de données
                String name = jsonObject.getString("name");
                int age = jsonObject.getInt("age");
                String city = jsonObject.getString("city");

                try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                     PreparedStatement pstmt = conn.prepareStatement("INSERT INTO users (name, age, city) VALUES (?, ?, ?)")) {
                    pstmt.setString(1, name);
                    pstmt.setInt(2, age);
                    pstmt.setString(3, city);
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
