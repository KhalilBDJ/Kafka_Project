package org.example;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;


import java.util.Scanner;


@Component
public class CommandProducer {

    private static final String TOPIC_NAME2 = "Topic2";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    @Async
    public void StartProducer() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Entrez une commande: ");
        while (true) {
            String input = scanner.nextLine();
            if (input.equals("help")) {
                System.out.println("""
                        Voici les commandes disponibles :\s
                        Get_global_values : retourne les valeurs globales clés Global du fichier json\s
                        Get_country_values <Pays> : retourne les valeurs du pays demandé\s
                        Get_confirmed_avg : retourne une moyenne des cas confirmés\s
                        Get_deaths_avg : retourne une moyenne des Décès\s
                        Get_countries_deaths_percent : retourne le pourcentage de Décès par rapport aux cas confirmés""");
            } else if (input.equals("exit")) {
                break;
            } else {
                kafkaTemplate.send(TOPIC_NAME2, input);
            }
        }
    }

}
