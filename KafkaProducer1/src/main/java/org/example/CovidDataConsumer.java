package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;


import java.sql.*;
import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.stereotype.Component;


@Component
public class CovidDataConsumer implements ConsumerSeekAware {

    private final static String TOPIC_NAME = "Topic1";
    private final static String GROUP_ID = "group1";
    private final static String DB_URL = "jdbc:postgresql://localhost:5432/kafkadb";
    private final static String DB_USER = "postgres";
    private final static String DB_PASSWORD = "1797"; // mettre son propre mot de passe


    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
            callback.seekToEnd(entry.getKey().topic(), entry.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {

    }


    @KafkaListener(topics = TOPIC_NAME, groupId = GROUP_ID)
    public void listen(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();

        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", DB_USER, DB_PASSWORD)) {
            Statement stmt = conn.createStatement();

            ResultSet resultSet = stmt.executeQuery("SELECT datname FROM pg_database WHERE datname = 'kafkadb'");
            if (!resultSet.next()) {
                stmt.executeUpdate("CREATE DATABASE kafkadb");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            JsonNode jsonNode = mapper.readTree(jsonString);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Global (id SERIAL PRIMARY KEY, new_confirmed INTEGER, total_confirmed INTEGER, new_deaths INTEGER, total_deaths INTEGER, new_recovered INTEGER, total_recovered INTEGER)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Countries (id SERIAL PRIMARY KEY, country TEXT, country_code TEXT, slug TEXT, new_confirmed INTEGER, total_confirmed INTEGER, new_deaths INTEGER, total_deaths INTEGER, new_recovered INTEGER, total_recovered INTEGER, date_maj TIMESTAMP)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Summary (id SERIAL PRIMARY KEY, country_id INTEGER, global_id INTEGER, date TIMESTAMP, FOREIGN KEY (country_id) REFERENCES Countries(id), FOREIGN KEY (global_id) REFERENCES Global(id))");
            PreparedStatement insertGlobalStmt = conn.prepareStatement("INSERT INTO Global (new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered) VALUES (?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement insertCountryStmt = conn.prepareStatement("INSERT INTO Countries (country, country_code, slug, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date_maj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            PreparedStatement insertSummaryStmt = conn.prepareStatement("INSERT INTO Summary (country_id, global_id, date) VALUES (?, ?, ?)");
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
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
