package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


@Component
public class CommandProducer {

    private static final String TOPIC_NAME2 = "Topic2";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    @Async
    public void StartProducer() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter a command to send to Kafka: ");
        while (true) {
            String input = scanner.nextLine();
            if (input.equals("help")) {
                System.out.println("Voici les commandes disponibles : \n" +
                        "Get_global_values : retourne les valeurs globales clés Global du fichier json \n" +
                        "Get_country_values <Pays> : retourne les valeurs du pays demandé \n" +
                        "Get_confirmed_avg : retourne une moyenne des cas confirmés \n" +
                        "Get_deaths_avg : retourne une moyenne des Décès \n" +
                        "Get_countries_deaths_percent : retourne le pourcentage de Décès par rapport aux cas confirmés");
            } else if (input.equals("exit")) {
                break;
            } else {
                kafkaTemplate.send(TOPIC_NAME2, input);
            }
        }
    }

}
