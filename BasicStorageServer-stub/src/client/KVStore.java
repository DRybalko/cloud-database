package client;

import java.io.IOException;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.*;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private String address;
	private int port;
	
	private CommunicationModule communicationModule;
	private Logger logger;

	public KVStore(String address, int port) {
		logger = Logger.getRootLogger();
		this.address = address;
		this.port = port;
	}
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @throws IllegalArgumentException 
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */

	public void connect() throws IllegalArgumentException, IOException {
		communicationModule = new CommunicationModule(address, port);
	}

	public void disconnect() {
		try {
			communicationModule.disconnectFromServer();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public KVMessage put(String key, String value) throws Exception {
		if (communicationModule.isClosed()) throw new Exception("No connection!");
		KVMessage kvMessage = new KVMessageItem(StatusType.PUT, key, value);
		return sendMessageWithErrorType(kvMessage, StatusType.PUT_ERROR);
	}

	public KVMessage get(String key) {
		KVMessage kvMessage = new KVMessageItem(StatusType.GET, key);
		return sendMessageWithErrorType(kvMessage, StatusType.GET_ERROR);
	}

	private KVMessage sendMessageWithErrorType(KVMessage kvMessage, StatusType errorType) {
		try {
			communicationModule.send(Marshaller.marshal(kvMessage));
			byte[] recievedMessage = communicationModule.receive();
			return Marshaller.unmarshal(recievedMessage);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return new KVMessageItem(errorType);
	}
	
/*	public static void main(String[] args) throws Exception {
		try {
			LogSetup logSetup = new LogSetup("logs/client/client.log", Level.ERROR);
		} catch (IOException e) {
			System.out.println("Logger could not be initialized");
		}
		KVStore store = new KVStore("localhost", 50000);
		store.connect();
		System.out.println(store.put("1000", "Hi").getStatus().toString());
		System.out.println(store.put("1001", "Hello").getStatus().toString());
		System.out.println(store.put("1002", "Welcome").getStatus().toString());
		System.out.println(store.get("1000").getStatus().toString() + ":" + store.get("1000").getValue());
		System.out.println(store.get("1001").getStatus().toString() + ":" + store.get("1001").getValue());
		System.out.println(store.put("1000", "Dima").getStatus().toString());
		System.out.println(store.get("1000").getStatus().toString() + ":" + store.get("1000").getValue());
		System.out.println(store.put("1005", "Sevastopol").getStatus().toString());
		System.out.println(store.put("1006", "Munich").getStatus().toString());
		System.out.println(store.put("1009", "Watermelon").getStatus().toString());
		System.out.println(store.get("1005").getStatus().toString() + ":" + store.get("1005").getValue());
		System.out.println(store.get("1000").getStatus().toString() + ":" + store.get("1000").getValue());
	}
	*/
}
