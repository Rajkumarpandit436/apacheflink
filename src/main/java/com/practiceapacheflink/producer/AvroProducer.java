package com.practiceapacheflink.producer;

import com.practiceapacheflink.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class AvroProducer {

    private static final String TOPIC = "flink-avro-input";

    public static void main(String[] args) {

        Properties properties = createKafkaProperties();

        KafkaProducer<String, User> producer =
                new KafkaProducer<>(properties);

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(
                () -> produceMessage(producer),
                0,
                5,
                TimeUnit.SECONDS
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println("Shutting down producer...");

                    scheduler.shutdown();

                    producer.close();

                    System.out.println("Producer stopped.");
                })
        );
    }

    private static Properties createKafkaProperties() {

        Properties properties = new Properties();

        properties.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        properties.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                "value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer"
        );

        properties.put(
                "schema.registry.url",
                "http://localhost:8081"
        );

        return properties;
    }

    private static void produceMessage(
            KafkaProducer<String, User> producer) {

        try {

            int id = ThreadLocalRandom.current()
                    .nextInt(1, 100000);

            User user = new User(
                    id,
                    "John Doe " + id,
                    "john" + id + "@example.com"
            );

            String key = id + "-"
                    + ThreadLocalRandom.current().nextInt();

            ProducerRecord<String, User> record =
                    new ProducerRecord<>(
                            TOPIC,
                            key,
                            user
                    );

            producer.send(
                    record,
                    (metadata, exception) -> {

                        if (exception != null) {

                            System.err.println(
                                    "Failed to send: "
                                            + exception.getMessage()
                            );

                        } else {

                            System.out.println(
                                    "Sent successfully: "
                                            + "partition="
                                            + metadata.partition()
                                            + ", offset="
                                            + metadata.offset()
                            );
                        }
                    }
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}