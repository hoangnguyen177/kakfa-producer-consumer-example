package com.gaurav.kafka;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.consumer.ConsumerCreator;
import com.gaurav.kafka.pojo.CustomObject;

public class App {
	public static void main(String[] args) {
//		runProducer();
		runConsumer();
	}

	static void runConsumer() {
		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;
		System.out.println("---------------Start-------------------");
		while (true) {
			final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(10000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}


}
