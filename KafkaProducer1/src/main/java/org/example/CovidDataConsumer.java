package org.example;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.Classes.Countries;
import org.example.Classes.Global;
import org.example.Classes.Summary;
import org.json.JSONException;
import org.json.JSONObject;

public class CovidDataConsumer {

    private final static String TOPIC_NAME = "Topic1";
    private final static String GROUP_ID = "group1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String DB_URL = "jdbc:postgresql://localhost:5432/kafkadb";
    private final static String DB_USER = "postgres";
    private final static String DB_PASSWORD = "1797";

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Arrays.asList(TOPIC_NAME));

        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(TOPIC_NAME)) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(partitions);

        // Seek to the end of each partition
        consumer.seekToEnd(partitions);

        ObjectMapper mapper = new ObjectMapper();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Global (id SERIAL PRIMARY KEY, new_confirmed INTEGER, total_confirmed INTEGER, new_deaths INTEGER, total_deaths INTEGER, new_recovered INTEGER, total_recovered INTEGER)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Countries (id SERIAL PRIMARY KEY, country TEXT, country_code TEXT, slug TEXT, new_confirmed INTEGER, total_confirmed INTEGER, new_deaths INTEGER, total_deaths INTEGER, new_recovered INTEGER, total_recovered INTEGER, date_maj TIMESTAMP)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Summary (id SERIAL PRIMARY KEY, country_id INTEGER, global_id INTEGER, date TIMESTAMP, FOREIGN KEY (country_id) REFERENCES Countries(id), FOREIGN KEY (global_id) REFERENCES Global(id))");
            PreparedStatement insertGlobalStmt = conn.prepareStatement("INSERT INTO Global (new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered) VALUES (?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement insertCountryStmt = conn.prepareStatement("INSERT INTO Countries (country, country_code, slug, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date_maj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement insertSummaryStmt = conn.prepareStatement("INSERT INTO Summary (country_id, global_id, date) VALUES (?, ?, ?)");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        JsonNode jsonNode = mapper.readTree(record.value());
                        System.out.println(record.value());
                        JsonNode globalNode = jsonNode.get("Global");
                        int newConfirmed = globalNode.get("NewConfirmed").asInt();
                        int totalConfirmed = globalNode.get("TotalConfirmed").asInt();
                        int newDeaths = globalNode.get("NewDeaths").asInt();
                        int totalDeaths = globalNode.get("TotalDeaths").asInt();
                        int newRecovered = globalNode.get("NewRecovered").asInt();
                        int totalRecovered = globalNode.get("TotalRecovered").asInt();
                        insertGlobalStmt.setInt(1, newConfirmed);
                        insertGlobalStmt.setInt(2, totalConfirmed);
                        insertGlobalStmt.setInt(3, newDeaths);
                        insertGlobalStmt.setInt(4, totalDeaths);
                        insertGlobalStmt.setInt(5, newRecovered);
                        insertGlobalStmt.setInt(6, totalRecovered);
                        insertGlobalStmt.executeUpdate();
                        ResultSet generatedKeys = insertGlobalStmt.getGeneratedKeys();
                        if (generatedKeys.next()) {
                            int globalId = generatedKeys.getInt(1);
                            ArrayNode countriesNode = (ArrayNode) jsonNode.get("Countries");
                            for (JsonNode countryNode : countriesNode) {
                                String country = countryNode.get("Country").asText();
                                String countryCode = countryNode.get("CountryCode").asText();
                                String slug = countryNode.get("Slug").asText();
                                int newConfirmedCountry = countryNode.get("NewConfirmed").asInt();
                                int totalConfirmedCountry = countryNode.get("TotalConfirmed").asInt();
                                int newDeathsCountry = countryNode.get("NewDeaths").asInt();
                                int totalDeathsCountry = countryNode.get("TotalDeaths").asInt();
                                int newRecoveredCountry = countryNode.get("NewRecovered").asInt();
                                int totalRecoveredCountry = countryNode.get("TotalRecovered").asInt();
                                Instant date = Instant.parse(countryNode.get("Date").asText());
                                insertCountryStmt.setString(1, country);
                                insertCountryStmt.setString(2, countryCode);
                                insertCountryStmt.setString(3, slug);
                                insertCountryStmt.setInt(4, newConfirmedCountry);
                                insertCountryStmt.setInt(5, totalConfirmedCountry);
                                insertCountryStmt.setInt(6, newDeathsCountry);
                                insertCountryStmt.setInt(7, totalDeathsCountry);
                                insertCountryStmt.setInt(8, newRecoveredCountry);
                                insertCountryStmt.setInt(9, totalRecoveredCountry);
                                insertCountryStmt.setTimestamp(10, Timestamp.from(date));
                                insertCountryStmt.executeUpdate();
                                ResultSet generatedKeysCountry = insertCountryStmt.getGeneratedKeys();
                                if (generatedKeysCountry.next()) {
                                    int countryId = generatedKeysCountry.getInt(1);
                                    insertSummaryStmt.setInt(1, countryId);
                                    insertSummaryStmt.setInt(2, globalId);
                                    insertSummaryStmt.setTimestamp(3, Timestamp.from(date));
                                    insertSummaryStmt.executeUpdate();
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }

    }
}

