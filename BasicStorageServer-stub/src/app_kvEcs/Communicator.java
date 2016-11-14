package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import common.messages.ECSMessage;
import common.messages.ECSMessageMarshaller;
import common.messages.MessageType;

public class Communicator {
	
	private final int TIMEOUT = 1000;
	
	//Map that contains socket for each server
	private Map<String, Socket> serverSockets;
	private Logger logger;
	private ECSMessageMarshaller marshaller;
	
	public Communicator() {
		this.serverSockets = new HashMap<>();
		this.logger = Logger.getRootLogger();
		this.marshaller = new ECSMessageMarshaller();
	}
	
	public ECSMessage sendMessage(KVServerItem server, ECSMessage message) {
		Socket socket;
		if (serverSockets.containsKey(server.getName())) socket = serverSockets.get(server.getName());
		else socket = createNewSocketFor(server);
		ECSMessage reply = null;
		try {
			OutputStream output = socket.getOutputStream();
			output.write(marshaller.marshal(message));
			output.flush();
			InputStream input = socket.getInputStream();
			reply = marshaller.unmarshal(readReply(input));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}	
		return reply;
	}
	
	private Socket createNewSocketFor(KVServerItem server) {
		Socket socket = null;
		try {
			socket = new Socket(server.getIp(), Integer.parseInt(server.getPort()));
			OutputStream output = socket.getOutputStream();
			output.write(MessageType.ECS.toString().getBytes());
			output.write((byte) 31);
			output.flush();
			serverSockets.put(server.getName(), socket);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return socket;
	}
	
	private byte[] readReply(InputStream input) throws IOException {
		List<Byte> readMessage = new ArrayList<>();
		int readByte = input.read();
		readMessage.add((byte)readByte);
		while (input.available() > 0) {
			readMessage.add((byte) input.read());
		}
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
	
	
	public boolean checkStarted(KVServerItem server) {
		try (Socket socket = new Socket()) {
	        socket.connect(new InetSocketAddress(server.getIp(), Integer.parseInt(server.getPort())), TIMEOUT);
	        socket.close();
	        return true;
	    } catch (IOException e) {
	        return false; 
	    }
	}
	
}