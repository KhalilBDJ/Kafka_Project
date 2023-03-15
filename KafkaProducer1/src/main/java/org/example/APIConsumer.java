package org.example;


import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class APIConsumer implements ConsumerSeekAware {

    private final APIProducer apiProducer;

    public APIConsumer(APIProducer apiProducer) {
        this.apiProducer = apiProducer;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
        for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
            callback.seekToEnd(entry.getKey().topic(), entry.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
        // Laissez cette méthode vide
    }
    @KafkaListener(topics = "Topic2", groupId = "group1")
    public void consume(String message) {
        String response = kafkaInterface(message);
        if (!response.isEmpty()) {
            apiProducer.sendMessage(response);
        }
    }

    private String kafkaInterface(String input){
        String outpout = "";
        if (input != null){
            switch (input){
                case "Get_global_values":
                    outpout = "Voici les global values";
                    break;
                case "Get_country_values Algerie":
                    outpout="DZ Power";
                    break;
                case "Get_confirmed_avg":
                    outpout = "La moyenne des confirmés est";
                    break;
                case "Get_deaths_avg":
                    outpout = "La moyenne des morts est";
                    break;
                case "Get_countries_deaths_percent":
                    outpout = "Le pourcentage de mort pour le pays est";
                    break;
            }
        }
        return outpout;
    }
}
