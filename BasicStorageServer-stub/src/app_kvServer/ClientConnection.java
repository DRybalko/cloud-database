package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.*;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Marshaller;


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
	private int MAX_MESSAGE_SIZE = 128000;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private PersistenceLogic persistenceLogic;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, PersistenceLogic persistenceLogic) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.persistenceLogic = persistenceLogic;
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
				closeConnection();
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private void receiveAndProcessMessage() throws IOException {
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();	
		while(isOpen) {
			try {
				if (input.available() > 0) {
					KVMessage receivedMessage = receiveMessage();
					KVMessage returnMessage = processMessage(receivedMessage);
					sendMessage(returnMessage);
				}
			} catch (IOException ioe) {
				logger.error("Error! Connection lost!");
				isOpen = false;
			}				
		}
	}

	private KVMessage processMessage(KVMessage message) {
		if (message.getStatus().equals(StatusType.GET)) {
			KVMessage getResult = persistenceLogic.get(message.getKey());
			return getResult;
		} else if (message.getStatus().equals(StatusType.PUT)) {
			return persistenceLogic.put(message.getKey(), message.getValue());
		} else {
			logger.error("Unnown message status, can not be proceeded.");
			return null;
		}
	}

	private void closeConnection() throws IOException {
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
		}
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVMessage message) throws IOException {
		byte[] msgBytes = Marshaller.marshal(message);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ "key: " + message.getKey() 
				+ ", value: " + message.getValue() + "'");
	}

	private KVMessage receiveMessage() throws IOException {
		List<Byte> readMessage = new ArrayList<>();
		int readByte = input.read();
		readMessage.add((byte)readByte);
		while (input.available() > 0 && readMessage.size() <= MAX_MESSAGE_SIZE) {
			readMessage.add((byte) input.read());
		}
		logger.debug("Recieved message in byte: " + readMessage);
		byte[] receivedMessage = convertToByteArray(readMessage);

		KVMessage msg = Marshaller.unmarshal(receivedMessage);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getStatus().toString() + "'");
		return msg;
	}	

	private byte[] convertToByteArray(List<Byte> list) {
		byte[] convertedArray = new byte[list.size()];
		Iterator<Byte> iterator = list.iterator();
		for (int i = 0; i < list.size(); i++) {
			convertedArray[i] = iterator.next();
		}
		return convertedArray;
	}
	
}