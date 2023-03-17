package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
@Import(KafkaConfiguration.class)
public class Main {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Main.class, args);
        String[] command;

        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            command = new String[]{"cmd.exe", "/c", "start", "console.bat"};
        } else {
            command = new String[]{"bash", "-c", "./console.sh"};
        }

        ProcessBuilder processBuilder = new ProcessBuilder(command);
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
