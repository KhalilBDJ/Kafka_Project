package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
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
            switch (input) {
                case "Get_global_values" -> output = objectMapper.writeValueAsString(globalRepository.findAll());
                case "Get_country_values Algerie" -> output = objectMapper.writeValueAsString(countriesRepository.findAll());
                case "Get_confirmed_avg" -> output = "La moyenne des confirmés est";
                case "Get_deaths_avg" -> output = "La moyenne des morts est";
                case "Get_countries_deaths_percent" -> output = "Le pourcentage de mort pour le pays est";
            }
        }
        return output;
    }

}
