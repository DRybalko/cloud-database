package client;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import common.messages.*;
import common.messages.KVMessage.KvStatusType;

/**
 * This class is responsible for the communication with the server.
 * It provides methods for establishing connection to the KV Server,
 * disconnecting the client from the currently connected server, 
 * inserting a key-value pair into the KVServer and retrieving 
 * the value for a given key from the server.
 * 
 * @see KVCommInterface
 * @see CommunicationModule
 *
 */

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
		KVMessage kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
		return sendMessageWithErrorType(kvMessage, KvStatusType.PUT_ERROR);
	}

	public KVMessage get(String key) {
		KVMessage kvMessage = new KVMessageItem(KvStatusType.GET, key);
		return sendMessageWithErrorType(kvMessage, KvStatusType.GET_ERROR);
	}

	private KVMessage sendMessageWithErrorType(KVMessage kvMessage, KvStatusType errorType) {
		KVMessageMarshaller kVMessageMarshaller = new KVMessageMarshaller();
		try {
			communicationModule.send(kVMessageMarshaller.marshal(kvMessage));
			byte[] recievedMessage = communicationModule.receive();
			return kVMessageMarshaller.unmarshal(recievedMessage);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return new KVMessageItem(errorType);
	}
}
