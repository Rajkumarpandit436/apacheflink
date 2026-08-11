package com.practiceapacheflink;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.HashMap;

public class UserDeserializationSchema implements DeserializationSchema<User> {

    private transient KafkaAvroDeserializer deserializer;

    @Override
    public void open(InitializationContext context) throws Exception {
        HashMap<String, Object> config = new HashMap<>();

        config.put(
                "schema.registry.url",
                "http://localhost:8081"
        );

        config.put(
                "specific.avro.reader",
                true
        );

        SchemaRegistryClient schemaRegistryClient =
                new CachedSchemaRegistryClient(
                        "http://localhost:8081",
                        100
                );

        deserializer = new KafkaAvroDeserializer(
                schemaRegistryClient,
                config
        );
    }

    @Override
    public User deserialize(byte[] message) throws IOException {
        return (User) deserializer.deserialize(
                "flink-input",
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
