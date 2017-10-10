package com.api.streams.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;

import com.api.streams.domains.Person;
import com.api.streams.producer.ProducerUtilities;

public class SparkConsumer {

	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static JavaStreamingContext ssc;
	private static final Map<String, String> kafkaParams = new HashMap<String, String>();

	public static void execute() throws InterruptedException {

		conf = new SparkConf().setAppName("Kafka-Spark-Streaming-Application")
				.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		ssc = new JavaStreamingContext(sc, new Duration(1000));

		kafkaParams
				.put("metadata.broker.list",
						"cstg-sa-stg-drc-03:9092,cstg-sa-stg-drc-04:9092,cstg-sa-stg-drc-05:9092");
		Set<String> topics = Collections.singleton("jason");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		directKafkaStream
				.foreachRDD(rdd -> {
					System.out.println("Rdd with Partition size "+rdd.partitions().size());
					System.out.println("Rdd with Record count "+rdd.count());
					
					rdd.foreach(record -> System.out.println("The record1 is "+record._1+" The record2 is "+record._2));
				});
		ssc.start();
		ssc.awaitTermination();
	}
}
