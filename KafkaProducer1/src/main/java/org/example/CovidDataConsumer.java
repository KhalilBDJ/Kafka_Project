package org.example;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.Classes.Countries;
import org.example.Classes.Global;
import org.json.JSONException;
import org.json.JSONObject;

public class CovidDataConsumer {

    private static final String TOPIC_NAME = "testTopic";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/kafkadb";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "1797";

    public static void main(String[] args) throws JSONException, SQLException, JsonProcessingException {

        // Configurer les propriétés du consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Créer le consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Abonner le consumer au topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        String url = "jdbc:postgresql://localhost:5432/kafkadb";
        String user = "postgres";
        String password = "1797";
        Connection conn = DriverManager.getConnection(url, user, password);

        ObjectMapper objectMapper = new ObjectMapper();

        // Lecture des messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // Convertir le message JSON en objet Java
                String message = record.value();
                JsonNode jsonNode = objectMapper.readTree(message);
                String globalString = jsonNode.get("Global").toString();
                Global global = objectMapper.readValue(globalString, Global.class);
                JsonNode countriesNode = jsonNode.get("Countries");
                Countries[] countries = objectMapper.readValue(countriesNode.toString(), Countries[].class);


                PreparedStatement statementGlobal = conn.prepareStatement("INSERT INTO global_data (new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered) VALUES (?, ?, ?, ?, ?, ?)");
                statementGlobal.setInt(1, global.getNewConfirmed());
                statementGlobal.setInt(2, global.getTotalConfirmed());
                statementGlobal.setInt(3, global.getNewDeaths());
                statementGlobal.setInt(4, global.getTotalDeaths());
                statementGlobal.setInt(5, global.getNewRecovered());
                statementGlobal.setInt(6, global.getTotalRecovered());
                statementGlobal.executeUpdate();

                // Insérer les données de chaque pays dans la table "country_data" de la base de données PostgreSQL
                for (Countries country : countries) {
                    // Insérer les données du pays dans la table "country_data" de la base de données PostgreSQL
                    PreparedStatement statementCountry = conn.prepareStatement("INSERT INTO country_data (country_code, country_name, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    statementCountry.setString(1, country.getCountryCode());

        // Boucle infinie de consommation des messages
                    statementCountry.setString(2, country.getCountry());
                    statementCountry.setInt(3, country.getNewConfirmed());
                    statementCountry.setInt(4, country.getTotalConfirmed());
                    statementCountry.setInt(5, country.getNewDeaths());
                    statementCountry.setInt(6, country.getTotalDeaths());
                    statementCountry.setInt(7, country.getNewRecovered());
                    statementCountry.setInt(8, country.getTotalRecovered());
                    statementCountry.setTimestamp(9, Timestamp.valueOf(String.valueOf(country.getDatemaj().getDay())));
                    statementCountry.executeUpdate();
                }
                conn.commit();
            }
        }
    }
}

