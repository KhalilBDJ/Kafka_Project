package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.transaction.Transactional;
import org.apache.kafka.common.TopicPartition;
import org.example.Classes.Countries;
import org.example.Classes.Global;
import org.example.Classes.Summary;
import org.example.Repositories.CountriesRepository;
import org.example.Repositories.GlobalRepository;
import org.example.Repositories.SummaryRepository;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private GlobalRepository globalRepository;

    @Autowired
    private CountriesRepository countriesRepository;

    @Autowired
    private SummaryRepository summaryRepository;

    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {
        for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
            callback.seekToEnd(entry.getKey().topic(), entry.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(@NotNull Map<TopicPartition, Long> assignments, @NotNull ConsumerSeekCallback callback) {

    }

    @Transactional
    @KafkaListener(topics = TOPIC_NAME, groupId = GROUP_ID)
    public void listen(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            globalRepository.deleteAll();
            summaryRepository.deleteAll();
            countriesRepository.deleteAll();
            JsonNode jsonNode = mapper.readTree(jsonString);
            JsonNode globalNode = jsonNode.get("Global");
            int newConfirmed = globalNode.get("NewConfirmed").asInt();
            int totalConfirmed = globalNode.get("TotalConfirmed").asInt();
            int newDeaths = globalNode.get("NewDeaths").asInt();
            int totalDeaths = globalNode.get("TotalDeaths").asInt();
            int newRecovered = globalNode.get("NewRecovered").asInt();
            int totalRecovered = globalNode.get("TotalRecovered").asInt();

            Global global = new Global();
            global.setNewConfirmed(newConfirmed);
            global.setTotalConfirmed(totalConfirmed);
            global.setNewDeaths(newDeaths);
            global.setTotalDeaths(totalDeaths);
            global.setNewRecovered(newRecovered);
            global.setTotalRecovered(totalRecovered);


            global = globalRepository.save(global);

            ArrayNode countriesNode = (ArrayNode) jsonNode.get("Countries");
            for (JsonNode countryNode : countriesNode) {
                String countryid = countryNode.get("ID").asText();
                String countryName = countryNode.get("Country").asText();
                String countryCode = countryNode.get("CountryCode").asText();
                String slug = countryNode.get("Slug").asText();
                int newConfirmedCountry = countryNode.get("NewConfirmed").asInt();
                int totalConfirmedCountry = countryNode.get("TotalConfirmed").asInt();
                int newDeathsCountry = countryNode.get("NewDeaths").asInt();
                int totalDeathsCountry = countryNode.get("TotalDeaths").asInt();
                int newRecoveredCountry = countryNode.get("NewRecovered").asInt();
                int totalRecoveredCountry = countryNode.get("TotalRecovered").asInt();
                Instant date = Instant.parse(countryNode.get("Date").asText());

                Countries country = new Countries();
                country.setId(countryid);
                country.setCountry(countryName);
                country.setCountryCode(countryCode);
                country.setSlug(slug);
                country.setNewConfirmed(newConfirmedCountry);
                country.setTotalConfirmed(totalConfirmedCountry);
                country.setNewDeaths(newDeathsCountry);
                country.setTotalDeaths(totalDeathsCountry);
                country.setNewRecovered(newRecoveredCountry);
                country.setTotalRecovered(totalRecoveredCountry);
                country.setDatemaj(Timestamp.from(date));
                country = countriesRepository.save(country);

                Summary summary = new Summary();
                summary.setCountries(country);
                summary.setGlobal(global);
                summary.setDate(Timestamp.from(date));

                summaryRepository.save(summary);
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
