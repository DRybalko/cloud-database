package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.log4j.Logger;

import common.messages.ECSMessage;
import common.messages.ECSMessage.EcsStatusType;
import common.messages.ECSMessageItem;
import common.messages.ECSMessageMarshaller;

public class EcsConnection implements Runnable{

	private Socket ecsSocket;
	private Logger logger;
	private InputStream input;
	private OutputStream output;
	private boolean isOpen;
	private ServerCommunicator<ECSMessage> communicator;
	private KVServer server;

	public EcsConnection(Socket ecs, KVServer server) {
		this.ecsSocket = ecs;
		this.logger = Logger.getRootLogger();
		this.isOpen = true;
		try {
			this.communicator = new ServerCommunicator<>(ecsSocket, new ECSMessageMarshaller());
		} catch (IOException e) {
			logger.error(e);
		}
		this.server = server;
	}

	public void run() {
		try {
			logger.debug("EcsConnection started thread...");
			receiveAndProcessMessage();
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);	
		} finally {			
			try {
				closeConnection();
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void receiveAndProcessMessage() throws IOException {
		output = ecsSocket.getOutputStream();
		input = ecsSocket.getInputStream();	
		while(isOpen) {
			try {
				if (input.available() > 0) {
					logger.debug("Start receiving message");
					ECSMessage receivedMessage = communicator.receiveMessage();
					logger.info("Received message status: " + receivedMessage.getStatus().toString());
					ECSMessage returnMessage = processMessage(receivedMessage);
					communicator.sendMessage(returnMessage);
					logger.debug("Message sent");
				}
			} catch (IOException ioe) {
				logger.error("Error! Connection lost!");
				isOpen = false;
			}				
		}
	}
	
	private ECSMessage processMessage(ECSMessage message) {
		if (message.getStatus().equals(EcsStatusType.START)){
			server.start();
			logger.debug("Got ECS request to start server. Server started");
		} else if (message.getStatus().equals(EcsStatusType.SERVER_START_END_INDEX)) {
			logger.debug("ECS request to initialize start and end index");
			server.setStartIndex(message.getStartIndex());
			server.setEndIndex(message.getEndIndex());
		} else if (message.getStatus().equals(EcsStatusType.META_DATA_TABLE)) {
			logger.debug("Receive meta data table");
			server.setMetaDataTable(message.getMetaDataTable());
		} else if (message.getStatus().equals(EcsStatusType.STOP)) {
			server.stop();
		} else if (message.getStatus().equals(EcsStatusType.SHUT_DOWN)) {
			server.shutDown();
			return new ECSMessageItem(EcsStatusType.DATA_TRANSFERED);
		} else if (message.getStatus().equals(EcsStatusType.UPDATE_START_INDEX)) {
			server.updateStartIndex(message.getStartIndex());
		}
		return new ECSMessageItem(EcsStatusType.REQUEST_ACCEPTED);
	}
	
	private void closeConnection() throws IOException {
		if (ecsSocket != null) {
			input.close();
			output.close();
			ecsSocket.close();
		}
	}
	
}
