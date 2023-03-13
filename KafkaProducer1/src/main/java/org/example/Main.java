package org.example;

import java.io.IOException;

public class Main {

    private static CommandProducer commandProducer;
    private static CommandConsumer commandConsumer;
    private static CovidDataConsumer covidDataConsumer;
    private static CovidDataProducer covidDataProducer;
    private static APIProducerAndConsumer apiProducerAndConsumer;
    public static void main(String[] args) throws IOException {
        covidDataConsumer.main(args);
        covidDataProducer.main(args);
        commandProducer.main(args);
        commandConsumer.main(args);
        apiProducerAndConsumer.main(args);
    }
}