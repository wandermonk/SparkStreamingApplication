package com.api.streams.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class ProducerUtilities {

	public static org.apache.kafka.clients.producer.Producer<String, String> getProducer() {
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
				"kafka json producer");
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"cstg-sa-stg-drc-03:9092,cstg-sa-stg-drc-04:9092,cstg-sa-stg-drc-05:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				ByteArraySerializer.class);
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);

		org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(
				configProperties);
		return producer;
	}

	public ProducerRecord<String, String> createRecord(String line) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				"jason", line);
		return record;
	}

}
