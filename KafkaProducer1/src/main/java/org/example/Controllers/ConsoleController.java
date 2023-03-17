package org.example.Controllers;

import org.example.CommandConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/")
public class ConsoleController {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CommandConsumer commandConsumer;

    public ConsoleController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("command")
    public ResponseEntity<String> sendCommand(@RequestBody String command) {
        kafkaTemplate.send("Topic2", command);
        String receivedMessage;
        try {
            receivedMessage = commandConsumer.getMessageFuture().get();
            return ResponseEntity.ok("Commande envoyée avec succès ! " + command + ". Message reçu : " + receivedMessage);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Une erreur est survenue lors de la récupération du message.");
        }
    }
}
