package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class CovidDataProducer {

    public static void main(String[] args) {
        // configure producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create kafka producer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        // fetch covid data from API
        String url = "https://api.covid19api.com/summary";
        ObjectMapper objectMapper = new ObjectMapper();
        //CovidData covidData = objectMapper.readValue(new URL(url), CovidData.class);
        //String json = objectMapper.writeValueAsString(covidData);

        // send data to kafka
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", "CECI EST UN TEST");
        producer.send(record);

        // close producer
        producer.close();
    }
}
