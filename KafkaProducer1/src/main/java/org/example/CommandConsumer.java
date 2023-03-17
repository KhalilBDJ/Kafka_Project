package org.example;

import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class CommandConsumer implements ConsumerSeekAware {
    private static final String TOPIC_NAME = "Topic3";
    private final CompletableFuture<String> messageFuture = new CompletableFuture<>();

    @KafkaListener(topics = TOPIC_NAME, groupId = "group1")
    public void listen(String message) {
        messageFuture.complete(message);
    }

    public CompletableFuture<String> getMessageFuture() {
        return messageFuture;
    }

    @Override
    public void onPartitionsAssigned(@NotNull Map<TopicPartition, Long> assignments, ConsumerSeekAware.@NotNull ConsumerSeekCallback callback) {
        for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
            callback.seekToEnd(entry.getKey().topic(), entry.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(@NotNull Map<TopicPartition, Long> assignments, ConsumerSeekAware.@NotNull ConsumerSeekCallback callback) {

    }
}
