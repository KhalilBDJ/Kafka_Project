package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

@Configuration
@EnableKafka
@Import(KafkaConfiguration.class)
@Component
public class CommandProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME2 = "Topic2";


    public void startProducer(KafkaTemplate<String, String> kafkaTemplate) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter a command to send to Kafka: ");
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

                System.out.println("Sent command: " + input + " to Kafka topic: " + TOPIC_NAME2);
            }
        }
    }

    @Async
    public void Start() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(CommandProducer.class);
        KafkaTemplate<String, String> kafkaTemplate = context.getBean(KafkaTemplate.class);
        CommandProducer commandProducer = new CommandProducer();
        commandProducer.startProducer(kafkaTemplate);
    }
}
