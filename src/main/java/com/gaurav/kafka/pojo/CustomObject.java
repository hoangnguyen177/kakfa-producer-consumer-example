package com.gaurav.kafka.pojo;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.json.JSONObject;

public class CustomObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public short version;
	public short messagetype;
	public UUID uuid; 
	public int sequenceNumber; // should be unsgined int here
	public int numberOfSequences; // unsgined int
	public int messageSize;
	public ByteBuffer buffer;
	public JSONObject json;
}
