package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.*;

import common.logic.ServerCommunicator;
import common.messages.ECSMessageItem;
import common.messages.KVMessageItem;
import common.messages.Message;
import common.messages.Message.MessageType;
import common.messages.PingMessageItem;

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
	private ServerCommunicator communicator;
	private KVServer server;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
		try {
			this.communicator = new ServerCommunicator(clientSocket);
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
		this.SERVER_NAME = server.getServerStatusInformation().getServerName() + ":";
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
		InputStream input = clientSocket.getInputStream();	
		while(isOpen && server.getServerStatusInformation().isRunning()) {
			try {
				if (input.available() > 0) {
					Message receivedMessage = communicator.receiveMessage();
					Message returnMessage = processMessage(receivedMessage);
					communicator.sendMessage(returnMessage);
				}
			} catch (IOException ex) {
				logger.error(SERVER_NAME+"Error! Connection lost!");
				isOpen = false;
			}				
		}
	}
	
	private Message processMessage(Message message) {
		if (message.getMessageType().equals(MessageType.CLIENT_TO_SERVER)) {
			KVMessageProcessor kvMessageProcessor = new KVMessageProcessor(server);
			return kvMessageProcessor.processMessage((KVMessageItem) message);
		} else if (message.getMessageType().equals(MessageType.ECS_TO_SERVER)) {
			ECSMessageProcessor ecsMessageProcessor = new ECSMessageProcessor(server);
			return ecsMessageProcessor.processMessage((ECSMessageItem) message);
		} else if (message.getMessageType().equals(MessageType.SERVER_TO_SERVER)) {
			ServerToServerMessageProcessor serverToServerMessageProcessor = new ServerToServerMessageProcessor(server);
			return serverToServerMessageProcessor.processMessage((PingMessageItem) message);
		} else {
			logger.error("Unknown message type: " + message.getMessageType().toString());
			return null;
		}
	}
}