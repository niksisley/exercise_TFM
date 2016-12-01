package com.colobu.kafka;

/**
 * Created by nico on 28/11/16.
 */
import java.io.IOException;

public class DriverKafka {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Debe tener 'producer' o 'consumer' como argumentos ");
        }
        if (args[0].equals("producer")) {
            TwitterKafkaProducer.main(args);

        } else if (args[0].equals("consumer")) {
            KafkaConsumer.main(args);

        } else {
            throw new IllegalArgumentException("Argumento invalido " + args[0]);
        }
    }
}

