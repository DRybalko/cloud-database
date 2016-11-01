package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * The class NetworkCommunicator is responsible for sending the user's 
 * message to the server and receiving the echo answer from the server.
 * This class also includes the function that disconnects the user from 
 * the server.
 * 
 */

public class CommunicationModule {
	
	private Socket echoSocket;
	private InputStream inputStream;
	private OutputStream outputStream;
	private Logger logger;
	
	private final int CONNECTION_TIMEOUT = 2000;
	
	public CommunicationModule (String address, int port) throws IOException, IllegalArgumentException{
		logger = Logger.getRootLogger();
		logger.info("Try to establish connection with address: "+address+"/"+port);
		echoSocket = new Socket();
		echoSocket.connect(new InetSocketAddress(address, port), CONNECTION_TIMEOUT);
		logger.info("Connection established successfuly");
		inputStream = echoSocket.getInputStream();	
		outputStream = echoSocket.getOutputStream();
	}
	
	public void send(byte[] message) throws IOException {
		logger.debug("Message to send in byte: " + Arrays.toString(message));
		outputStream.write(message);
		outputStream.flush();
	}
	
	public byte[] receive() throws IOException {
		List<Byte> readMessage = new ArrayList<>();
		int readByte = inputStream.read();
		readMessage.add((byte)readByte);
		while (inputStream.available() > 0) {
			readMessage.add((byte) inputStream.read());
		}
		logger.debug("Recieved message in byte: " + readMessage);
		return convertToByteArray(readMessage);
	}
	
	private byte[] convertToByteArray(List<Byte> list) {
		byte[] convertedArray = new byte[list.size()];
		Iterator<Byte> iterator = list.iterator();
		for (int i = 0; i < list.size(); i++) {
			convertedArray[i] = iterator.next();
		}
		return convertedArray;
	}
	
	public void disconnectFromServer() throws IOException {
		inputStream.close();
		outputStream.close();
		echoSocket.close();
		logger.info("Connection to server was closed");
	}
	
	public boolean isClosed() {
		return echoSocket.isClosed();
	}
}
