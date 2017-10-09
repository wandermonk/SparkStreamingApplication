package com.api.streams.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;

public class SparkConsumer {

	private static SparkConf conf;
	private static JavaSparkContext sc;
	private static JavaStreamingContext ssc;
	private static final Map<String, String> kafkaParams = new HashMap<String, String>();

	public static void main(String[] args) throws InterruptedException {

		conf = new SparkConf().setAppName("Kafka-Spark-Streaming-Application")
				.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		ssc = new JavaStreamingContext(sc, new Duration(1000));

		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("test");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		directKafkaStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>,Time>(){

			/**
			 * serial version uuid
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, String> rdd, Time time)
					throws Exception {
				
				rdd.foreach(data -> {
					System.out.println(data._1 + "  " + data._2);
				});
			}
			
		});
		ssc.start();
		ssc.awaitTermination();

	}

}

