package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class CommandProducer {

    private static final String BOOSTSRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME2 = "Topic2";
    private static final String TOPIC_NAME3 = "Topic3";
    private final static String GROUP_ID = "group1";

    public static void main(String[] args) throws IOException{
        CommandProducer commandProducer = new CommandProducer();
        commandProducer.StartProducer();

    }

    private void StartProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTSRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter a command to send to Kafka: ");
            String input = scanner.nextLine();
            if (input.equals("help")){
                System.out.println("Voici les commandes disponibles : \n" +
                        "Get_global_values : retourne les valeurs globales clés Global du fichier json \n" +
                        "Get_country_values <Pays> : retourne les valeurs du pays demandé \n" +
                        "Get_confirmed_avg : retourne une moyenne des cas confirmés \n" +
                        "Get_deaths_avg : retourne une moyenne des Décès \n" +
                        "Get_countries_deaths_percent : retourne le pourcentage de Décès par rapport aux cas confirmés");
            } else if (input.equals("exit")) {
                producer.close();
            } else {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC_NAME2, input);

                producer.send(record);

                System.out.println("Sent command: " + input + " to Kafka topic: " + TOPIC_NAME2);
            }
        }
    }


}
