package com.practiceapacheflink;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.HashMap;

@Slf4j
public class UserDeserializationSchema implements DeserializationSchema<User> {

    private transient KafkaAvroDeserializer deserializer;

    @Override
    public void open(InitializationContext context) throws Exception {
        HashMap<String, Object> config = new HashMap<>();

        config.put(
                "schema.registry.url",
                "http://schema-registry:8081"
        );

        config.put(
                "specific.avro.reader",
                true
        );

        SchemaRegistryClient schemaRegistryClient =
                new CachedSchemaRegistryClient(
                        "http://schema-registry:8081",
                        100
                );

        deserializer = new KafkaAvroDeserializer(
                schemaRegistryClient,
                config
        );
    }

    @Override
    public User deserialize(byte[] message) throws IOException {

        log.info("Raw message length received: {}", message != null ? message.length : 0);

        return (User) deserializer.deserialize(
                "flink-avro-input",
                message
        );
    }

    @Override
    public boolean isEndOfStream(User nextElement) {
        return false;
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }
}
