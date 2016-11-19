package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

import org.apache.log4j.*;

import common.logic.ByteArrayMath;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.KVMessage;
import common.messages.KVMessage.KvStatusType;
import common.messages.KVMessageItem;
import common.messages.KVMessageMarshaller;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private final String SERVER_NAME;
	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private KVServer server;
	private ServerCommunicator<KVMessage> communicator;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
		try {
			this.communicator = new ServerCommunicator<KVMessage>(clientSocket, new KVMessageMarshaller());
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
		this.SERVER_NAME = server.getServerName() + ":";
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			receiveAndProcessMessage();
		} catch (IOException ioe) {
			logger.error(SERVER_NAME+"Error! Connection could not be established!", ioe);	
		} finally {			
			try {
				communicator.closeConnection();
			} catch (IOException ioe) {
				logger.error(SERVER_NAME+"Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void receiveAndProcessMessage() throws IOException {
		input = clientSocket.getInputStream();	
		while(isOpen && server.isRunning()) {
			try {
				if (input.available() > 0 && server.isAcceptingRequests()) {
					KVMessage receivedMessage = communicator.receiveMessage();
					logger.debug(SERVER_NAME+"Server with start index " + Arrays.toString(server.getStartIndex()) 
							+ " and end index " + Arrays.toString(server.getEndIndex())
							+ " received message: StatusType: "+receivedMessage.getStatus() 
							+ " with hash value " + Arrays.toString(HashGenerator.generateHashFor(receivedMessage.getKey())));
					addKeyToKeyList(receivedMessage);
					KVMessage returnMessage = processMessage(receivedMessage);
					logger.debug(SERVER_NAME+"Message to send: StatusType: "+returnMessage.getStatus());
					communicator.sendMessage(returnMessage);
				} else if (input.available() > 0 && !server.isAcceptingRequests()){
					communicator.sendMessage(new KVMessageItem(KvStatusType.SERVER_STOPPED));
					input.skip(input.available());
				} 
			} catch (IOException ex) {
				logger.error(SERVER_NAME+"Error! Connection lost!");
				isOpen = false;
			}				
		}
	}

	private void addKeyToKeyList(KVMessage receivedMessage) {
		logger.debug("Check if received message is put");
		if (receivedMessage.getStatus().equals(KvStatusType.PUT) && !receivedMessage.getValue().equals("null")) {
			logger.debug("Message is put");
			server.addKey(receivedMessage.getKey());
		} else if (receivedMessage.getStatus().equals(KvStatusType.PUT) && receivedMessage.getValue().equals("null")) {
			logger.debug("Message is not put");
			server.deleteKey(receivedMessage.getKey());
		}
	}
	
	private KVMessage processMessage(KVMessage message) {
		if (message.getStatus().equals(KvStatusType.GET)) {
			KVMessage getResult = server.getPersistenceLogic().get(message.getKey());
			return getResult;
			//TODO check is server can process message(not stopped) 
		} else if (message.getStatus().equals(KvStatusType.PUT)) {
			if (ByteArrayMath.isValueBetweenTwoOthers(HashGenerator.generateHashFor(message.getKey()), server.getStartIndex(), server.getEndIndex())) {
				return server.getPersistenceLogic().put(message.getKey(), message.getValue());
			} else {
				KVServerItem responsibleServer = findResponsibleServer(HashGenerator.generateHashFor(message.getKey()));
				KVMessageItem responseMessage =  new KVMessageItem(KvStatusType.SERVER_NOT_RESPONSIBLE);
				responseMessage.setServer(responsibleServer);
				return responseMessage;
			}
		} else {
			logger.error(SERVER_NAME+"Unnown message status, can not be proceeded.");
			return null;
		}
	}
	
	private KVServerItem findResponsibleServer(byte[] keyHash) {
		for (KVServerItem server: server.getMetaDataTable()) {
			if (ByteArrayMath.isValueBetweenTwoOthers(keyHash, server.getStartIndex(), server.getEndIndex())) {
				return server;
			}
		}
		return null;
	}
}