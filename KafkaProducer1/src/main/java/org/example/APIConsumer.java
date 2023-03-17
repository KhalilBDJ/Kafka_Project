package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;

import org.example.Classes.Countries;
import org.example.Classes.Global;
import org.example.Repositories.CountriesRepository;
import org.example.Repositories.GlobalRepository;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.Map;

@Component
public class APIConsumer implements ConsumerSeekAware {

    private final APIProducer apiProducer;

    @Autowired
    GlobalRepository globalRepository;

    @Autowired
    CountriesRepository countriesRepository;

    public APIConsumer(APIProducer apiProducer) {
        this.apiProducer = apiProducer;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.@NotNull ConsumerSeekCallback callback) {
        for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
            callback.seekToEnd(entry.getKey().topic(), entry.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(@NotNull Map<TopicPartition, Long> assignments, ConsumerSeekAware.@NotNull ConsumerSeekCallback callback) {

    }
    @KafkaListener(topics = "Topic2", groupId = "group1")
    public void consume(String message) throws JsonProcessingException {
        String response = kafkaInterface(message);
        if (!response.isEmpty()) {
            apiProducer.sendMessage(response);
        }
    }

    public String kafkaInterface(String input) throws JsonProcessingException {
        String output = "";
        ObjectMapper objectMapper = new ObjectMapper();
        if (input != null){
            String command = extractCommand(input);
            switch (command) {
                case "Get_global_values" -> output = "L'état global est " + objectMapper.writeValueAsString(globalRepository.findAll());
                case "Get_country_values" -> output = getCountryVallues(extractCountry(input));
                case "Get_confirmed_avg" -> output = "La moyenne des confirmés est " + getConfirmedAvg();
                case "Get_deaths_avg" -> output = "La moyenne des morts est " + getDeathsAvg();
                case "Get_countries_deaths_percent" -> output = "Le pourcentage de mort pour le pays est " + countriesDeathsPercent();
            }
        }
        return output;
    }


    public String getCountryVallues(String name) throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();

        String countryString = objectMapper.writeValueAsString(countriesRepository.findByCountry(name));
        Countries country = objectMapper.readValue(countryString, Countries.class);

        return countryString;
    }

    public String getConfirmedAvg() {
        ObjectMapper objectMapper = new ObjectMapper();

        List<Countries> countriesList = countriesRepository.findAll();
        //List<String> nameList = new ArrayList<>();
        double totalConfirmed = 0.0;

        for (Countries country : countriesList) {
            totalConfirmed += (double) country.getTotalConfirmed();
        }

        double average = totalConfirmed / countriesList.size();
        return Double.toString(average);

    }

    public String getDeathsAvg() {
        ObjectMapper objectMapper = new ObjectMapper();

        List<Countries> countriesList = countriesRepository.findAll();
        //List<String> nameList = new ArrayList<>();
        double totalConfirmed = 0.0;

        for (Countries country : countriesList) {
            totalConfirmed += (double) country.getTotalDeaths();
        }

        double average = totalConfirmed / countriesList.size();
        return Double.toString(average);
    }

    public String countriesDeathsPercent() throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();

        String globalString = objectMapper.writeValueAsString(countriesRepository.findAll());
        Global global = objectMapper.readValue(globalString, Global.class);

        double deathPercent = (double) global.getTotalDeaths() / (double) global.getTotalConfirmed();

        return Double.toString(deathPercent);
    }

    public String extractCountry(String input) {
        String[] parts = input.split("\\s+");
        return parts[1];
    }

    public String extractCommand(String input) {
        String[] parts = input.split("\\s+");
        return parts[0];
    }
}