
package com.eventbrite;

import java.io.IOException;
import java.util.HashMap;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitProducerTest implements PerfTestable{

	protected Address[] endpoints;
	protected ConnectionFactory connectionFactory = null;
    protected Connection connection = null;
    protected Channel channel = null;
    protected boolean usePubAcks = false;
    protected String queueName = "";
    protected String exchangeName = "eb.logs.web_analytics";
    protected String messageBody = "";
    protected int messageSize = 0;
    protected String messageFile = "";  // if set then file contents are read line by line.
    
	@Override
	public boolean setup(String config) {
		// TODO Auto-generated method stub
		// Comma separated values for various parameters.
		if (config == null) {
			config = "addressess=host:99999,host2:99999;pub-acks=false;queue=persistentperf";			
		}
		if (!parseConfig(config)) {
			return false;
		}
		try {
			connectionFactory = new ConnectionFactory();
			connectionFactory.setHost("dev-fh-mq1");
			connectionFactory.setPort(5672);
			connectionFactory.setUsername("syslog");
			connectionFactory.setPassword("loLLypop443");
			connectionFactory.setVirtualHost("/syslog");
			
	        System.out.println("connection done");
	        for (Address addr : endpoints) {
				System.out.println("Endpoint: " + addr.getHost() + ":" +
						Integer.toString(addr.getPort()));
			}

			// If not using Address[] then use below setters.
			//connectionFactory.setHost(params.host);connectionFactory.setPort(params.port);
	        connection = connectionFactory.newConnection(endpoints);
	        //connection = connectionFactory.newConnection();
	        System.out.println("Creating channel");
	        channel = connection.createChannel();
	        channel.basicQos(1);			
		} catch(IOException io) {
			System.out.println("IOException in setup(). " + io.toString());
			io.printStackTrace();
			freeResources();
			return false;
		}
		return true;
	}

	private void freeResources() {
		// TODO Auto-generated method stub
		try {
			if (channel != null) channel.close();
			if (connection != null) connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while freeing Rabbit resources. " +
					e.getMessage());
		}
		
	}

	private boolean parseConfig(String config) {
		// Add some defaults. Should be removed ideally.
		
		// Parse the config.
		String[] results = config.split(";");
		HashMap<String, String> configuration = new HashMap<String, String>(results.length);
		for (String kv : results) {
			String[] kvpair = kv.split("=");
			if (kvpair.length != 2) continue;
			configuration.put(kvpair[0], kvpair[1]);		
		}
		
		String addresses = configuration.get("addresses");
		if (addresses == null) addresses = "qa-fh-mq1:5672,qa-fh-mq2:5672,qa-fh-mq3:5672,qa-fh-mq4:5672";
		endpoints = Address.parseAddresses(addresses); 
		
		queueName = configuration.get("queue");
		if (queueName == null) queueName = "persistentperf";

		exchangeName = configuration.get("exchange");
		if (exchangeName == null) exchangeName = "";
		
		if (configuration.get("pub-acks") != null)
			usePubAcks = Boolean.getBoolean(configuration.get("pub-acks"));
		
		return true;
	}

	@Override
	public long work(long units) {
		// TODO Auto-generated method stub
		byte[] body;  // = "some message".getBytes();
		body = messageBody.getBytes();  // Have a field that stores bytes instead of string.
		for (long i = 0; i < units; ++i)	{
			try {
				channel.basicPublish(exchangeName, queueName,
						new AMQP.BasicProperties.Builder().contentType("text/plain")
							.deliveryMode(2).priority(1).build(),
						body);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Error in publishing. Messages sent so far: " + (i+1));
				return i+1;
			}
		}
		return units;
	}

	private byte[] getProducerMsg() {
		//int size = messageSize;
		
		return messageBody.getBytes();
	}

	@Override
	public boolean teardown() {
		freeResources();
		return true;
	}

	@Override
	public boolean setWorkItemFile(String fileName) {
		messageFile = fileName;
		// @TODO: check if file exists!!
		return true;
	}

	@Override
	public boolean useRandomString(int size) {
		if (size <= 0) return false;
		messageSize = size;
		String myRandomString = "HeheHaha..This string is 50 characters long.!!   :)";
		StringBuilder temp = new StringBuilder(size);
		for (int i = 0; i < (size/50); i++) {
			temp.append(myRandomString);
		}
		for (int i = 0; i < (size%50); i++) {
			temp.append(" ");
		}
		messageBody = temp.toString();
		return true;
	}
	
	public void setWorkItemString(String str) {
		messageBody = str;
	}

}
