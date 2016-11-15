package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.*;

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

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private KVServer server;
	private Communicator<KVMessage> communicator;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
		try {
			this.communicator = new Communicator<KVMessage>(clientSocket, new KVMessageMarshaller());
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			receiveAndProcessMessage();
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);	
		} finally {			
			try {
				communicator.closeConnection();
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void receiveAndProcessMessage() throws IOException {
		input = clientSocket.getInputStream();	
		while(isOpen) {
			try {
				if (input.available() > 0 && server.isAcceptingRequests()) {
					KVMessage receivedMessage = communicator.receiveMessage();
					logger.debug("Receive message: StatusType: "+receivedMessage.getStatus());
					KVMessage returnMessage = processMessage(receivedMessage);
					logger.debug("Message to send: StatusType: "+returnMessage.getStatus());
					communicator.sendMessage(returnMessage);
				} else if (input.available() > 0 && !server.isAcceptingRequests()){
					communicator.sendMessage(new KVMessageItem(KvStatusType.SERVER_STOPPED));
					input.skip(input.available());
				} 
			} catch (IOException ex) {
				logger.error("Error! Connection lost!");
				isOpen = false;
			}				
		}
	}

	private KVMessage processMessage(KVMessage message) {
		if (message.getStatus().equals(KvStatusType.GET)) {
			KVMessage getResult = server.getPersistenceLogic().get(message.getKey());
			return getResult;
			//TODO check is server can proccess message(not stopped) 
		} else if (message.getStatus().equals(KvStatusType.PUT)) {
			//if (server.checkIfInRand(String key)) {
				return server.getPersistenceLogic().put(message.getKey(), message.getValue());
			//} else {
				//TODO new KVMessage with information about server responsible for this key. Information must be taken from sever.getMetaDataTable()
			//}
		} else {
			logger.error("Unnown message status, can not be proceeded.");
			return null;
		}
	}
}