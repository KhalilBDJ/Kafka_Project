package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@Import(KafkaConfiguration.class)
@EnableAsync
public class Main {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Main.class, args);

        // Exécuter CovidDataProducer
        CovidDataProducer producer = context.getBean(CovidDataProducer.class);
        try {
            producer.Start(context);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Exécuter CovidDataConsumer
        CovidDataConsumer consumer = context.getBean(CovidDataConsumer.class);
        try {
            consumer.Start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Exécuter CommandProducer
        CommandProducer commandProducer = context.getBean(CommandProducer.class);
        try {
            commandProducer.Start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
