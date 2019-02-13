package com.gaurav.kafka.deserializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.gaurav.kafka.pojo.CustomObject;

public class CustomDeserializer implements Deserializer<CustomObject> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public CustomObject deserialize(String topic, byte[] data) {
		int headerLength = 2 + 2 + 38 + 12;
//	    if (data.length < headerLength) {
//	      System.out.println("[CustomeDeserialiser]Not valid message....");
//	      return null;
//	    }
		System.out.println("buffer length");
		System.out.println(data.length);
	    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
	    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
	    short version = byteBuffer.getShort();
	    System.out.println("version:" + version);
//	    byte[] versionArray = new byte[2];
//	    byteBuffer.get(versionArray);
//	    System.out.println("version:" + (new String(versionArray, StandardCharsets.UTF_8)));
//	    System.out.println("version:" + ByteBuffer.wrap(versionArray).getShort());
	    
	    short messageType = byteBuffer.getShort();
	    System.out.println("message type:" + messageType);
	    byte[] uidArray = new byte[38];
		byteBuffer.get(uidArray);
		System.out.println("uuid:" + (new String(uidArray, StandardCharsets.UTF_8)));
		int sequenceNumber = byteBuffer.getInt();
		System.out.println("sequenceNumber:" + sequenceNumber);
		int numberOfSequence = byteBuffer.getInt();
		System.out.println("numberOfSequence:" + numberOfSequence);
		int messageSize = byteBuffer.getInt();
		System.out.println("messageSize:" + messageSize);
		ByteBuffer payload = byteBuffer.slice();
		// metadata 
		if(messageType ==0) {
			String text = new String(payload.array(), StandardCharsets.UTF_8);
			System.out.println("Actual message");
			System.out.println(text);
		}
		
		CustomObject object = null;
		try {
			object = null;
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {
	}
}