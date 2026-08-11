package com.practiceapacheflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheflinkApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<User> source = KafkaSource.<User>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("flink-avro-input")
				.setGroupId("flink-user-consumer")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new UserDeserializationSchema())
				.build();
		DataStream<User> users =
				env.fromSource(
						source,
						WatermarkStrategy.noWatermarks(),
						"Kafka Source");
		users.print();

		env.execute("Flink Job");
	}

}
