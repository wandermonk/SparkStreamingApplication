package com.api.streams.KafkaSparkStreaming;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.api.streams.consumer.SparkConsumer;
import com.api.streams.producer.ProducerUtilities;


public class MainApp {
	
	private static ProducerUtilities producerutility = new ProducerUtilities();

	public static void main(String[] args) {

		final Thread producerThread = new Thread(new Runnable() {

			org.apache.kafka.clients.producer.Producer<String, String> producer = ProducerUtilities
					.getProducer();
			
			@Override
			public void run() {
				String s = "hello";
				for(int i=0;i<10000;i++){
					System.out.println("pushing record to the JASON topic");
					ProducerRecord<String, String> record = producerutility
							.createRecord(s);
					producer.send(record);
				}	
			}	
		});
		
		final Thread consumerThread = new Thread(new Runnable(){

			@Override
			public void run() {
				try {
					SparkConsumer.execute();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		producerThread.start();

		consumerThread.start();
	}

}
