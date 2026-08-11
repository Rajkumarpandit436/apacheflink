package com.practiceapacheflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheflinkApplication {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

		KafkaSource<User> source = KafkaSource.<User>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("flink-avro-input")
				.setGroupId("flink-user-consumer")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
				.setValueOnlyDeserializer(new UserDeserializationSchema())
				.build();

		DataStream<User> users =
				env.fromSource(
						source,
						WatermarkStrategy.noWatermarks(),
						"Kafka Source");

		users.sinkTo(
				new UserDBSink()
		);
		env.execute("Flink Job");
	}

}
