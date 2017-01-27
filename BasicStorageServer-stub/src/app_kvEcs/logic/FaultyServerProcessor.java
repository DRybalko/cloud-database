package app_kvEcs.logic;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.logic.KVServerItem;
import common.logic.ServerCommunicator;
import common.messages.ecsToServerMessage.ECSMessageItem;
import common.messages.ecsToServerMessage.ECSMessage.EcsStatusType;
import common.messages.Message;

/**
 * This class is responsible for the recovery mechanism, when faulty server is detected. In this case it 
 * updates meta data table (delete faulty server) and adds new node to the system. Replication data of the faulty
 * server is then send to the servers left in the meta data table.
 */
public class FaultyServerProcessor {

	private static final int CACHE_SIZE = 30;
	private static final String DISPLACEMENT_STRATEGY = "FIFO";
	
	private ECSLogic ecsLogic;
	private Logger logger;
	private ServerCommunicator communicator;
	private Socket clientSocket;
	
	public FaultyServerProcessor(Socket clientSocket, ECSLogic ecsLogic) {
		this.ecsLogic = ecsLogic;
		this.logger = Logger.getRootLogger();
		this.clientSocket = clientSocket;
		try {
			this.communicator = new ServerCommunicator(clientSocket);
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
	}
	
	public void processConnection() {
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
				Message returnMessage = processMessage((ECSMessageItem) receivedMessage);
				communicator.sendMessage(returnMessage);
			}
		} catch (IOException ex) {
			logger.error("Error! Connection lost!");
		}				
	}
	
	private Message processMessage(ECSMessageItem message) {
		KVServerItem faultyServerInMetaDataTable = ecsLogic.getMetaDataTableController()
				.findServerInMetaDataTable(message.getServerItem());
		ECSMessageItem replyMessage;
		if (faultyServerInMetaDataTable != null) {
			ecsLogic.removeFaultyServer(faultyServerInMetaDataTable);
			ecsLogic.addNode(CACHE_SIZE, DISPLACEMENT_STRATEGY);
			replyMessage = new ECSMessageItem(EcsStatusType.REALLOCATE_DATA);
		} else {
			replyMessage = new ECSMessageItem(EcsStatusType.SERVER_STOPPED);
		}
		logger.info("Data reallocated and server started");
		return replyMessage;
	}

}
