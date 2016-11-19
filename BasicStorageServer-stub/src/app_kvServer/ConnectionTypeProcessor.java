package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.ConnectionType;

public class ConnectionTypeProcessor implements Runnable  {

	private Logger logger;
	private KVServer server;
	private Socket client;
	
	public ConnectionTypeProcessor(KVServer server, Socket client) {
		this.server = server;
		this.client = client;
		this.logger = Logger.getRootLogger();
	}
	
	@Override
	public void run() {
		String messageHeader = "";
		try {
			messageHeader = getMessageHeader(client);
		} catch (IOException e) {
			e.printStackTrace();
		}	
		//Message Header is needed to proceed communication with ECS and Client in different ways. Can have values ECS or KV_MESSAGE
		logger.debug("message header is " + messageHeader);
		if (messageHeader.equals(ConnectionType.ECS.toString())) {
			logger.info(server.getServerName() + ":Connection to ECS established");
			EcsConnection connection = new EcsConnection(client, server);
			new Thread(connection).start();
		} else if (messageHeader.equals(ConnectionType.KV_MESSAGE.toString())){
			ClientConnection connection = new ClientConnection(client, server);
			new Thread(connection).start();
		}	
		logger.info(server.getServerName() + ":Connected to " 
				+ client.getInetAddress().getHostName() 
				+  " on port " + client.getPort());
	}
	
	/**
	 * 
	 * @param client with InputStream
	 * @return message header. Can be ECS or KV_CLIENT. Used to differentiate between two
	 * separate communication channels. 
	 * @throws IOException
	 */
	private String getMessageHeader(Socket client) throws IOException {
		InputStream input = client.getInputStream();
		StringBuilder sb =  new StringBuilder();
		byte symbol = (byte) input.read();
		logger.info(server.getServerName() + ":Checking input stream ...");
		while (input.available() > 0 && (symbol != (byte) 31)) {
			sb.append((char) symbol);
			symbol = (byte) input.read();
		}
		return sb.toString();
	}

}
