package client;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.logic.ServerCommunicator;
import common.messages.Message;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;
import common.messages.clientToServerMessage.KVMessageItem;

public class NotificationMessageProcessor implements Runnable {
	
	private Socket clientSocket;
	private Logger logger;
	private ServerCommunicator communicator;
	private KVStore kvStore;
	
	public NotificationMessageProcessor(Socket clientSocket, KVStore kvStore) {
		this.clientSocket = clientSocket;
		this.logger = Logger.getRootLogger();
		this.kvStore = kvStore;
		try {
			this.communicator = new ServerCommunicator(clientSocket);
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
	}
	
	public void run() {
		try {
			receiveAndProcessMessage();
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);	
		} finally {			
			try {
				communicator.closeConnection();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void receiveAndProcessMessage() throws IOException {
		InputStream input = clientSocket.getInputStream();	
		try {
			if (input.available() > 0) {
				Message receivedMessage = communicator.receiveMessage();
				Message returnMessage = this.processMessage((KVMessageItem) receivedMessage);
				communicator.sendMessage(returnMessage);
			}
		} catch (IOException ex) {
			logger.error("Error! Connection lost!");
		}				
	}
	
	private Message processMessage(KVMessageItem message) {
		if ((message.getValue().getPermission() < kvStore.getPermission()) || 
				(message.getValue().getPermission() == kvStore.getPermission() && message.getValue().getUsername().equals(kvStore.getUsername()))) 
			System.out.print("Key: " + message.getKey() + " was changed. New value for this key: " + message.getValue().getValue() + "\nClient> ");
		else System.out.println("No permission");
		return new KVMessageItem(KvStatusType.PUT_SUCCESS);
	}

}
