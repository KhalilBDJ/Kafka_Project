package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

import java.io.IOException;

@SpringBootApplication
@Import(KafkaConfiguration.class)
public class Main {



    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Main.class, args);
        String scriptName;

        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            scriptName = "console.bat";
        } else {
            scriptName = "./console.sh";
        }

        ProcessBuilder processBuilder = new ProcessBuilder(scriptName);
        try {
            Process process = processBuilder.start();
            process.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        CommandProducer commandProducer = context.getBean(CommandProducer.class);
        commandProducer.StartProducer();

    }
}
