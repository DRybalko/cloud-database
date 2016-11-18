package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.Arrays;
import java.util.List;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;
import app_kvServer.PersistenceLogic;
import logger.LogSetup;
import app_kvServer.Range;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.messages.ConnectionType;
import common.messages.KVMessageItem;
import common.messages.KVMessageMarshaller;
import common.messages.KVMessage.KvStatusType;


/**
 * Main class for the KVServer. Holds the variables needed for
 * the connection with the server (like the port number, the 
 * cache size and the cache strategy). It initializes and starts
 * the KVServer at given port and listens for clients until it is stopped.
 * 
 * @see PersistenceLogic
 */
public class KVServer {

	private ServerSocket serverSocket;
	private String serverName;
	private Logger logger;
	private int port;
	private boolean running;
	private PersistenceLogic persistenceLogic;
	private boolean writeLock;
	private boolean acceptingRequests;
	private boolean stopped;
	private byte[] startIndex;
	private byte[] endIndex;
	private List<String> keys;
	private List<KVServerItem> metaDataTable;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public static void main(String[] args) {
		try {
			processArgs(args);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.exit(1);
		}
	}

	private static void processArgs(String[] args) throws IOException {
		new LogSetup("/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/logs/server/server.log", Level.ALL);
		int port = Integer.parseInt(args[0]);
		int cacheSize = Integer.parseInt(args[1]);
		String cacheStrategy = args[2];
		KVServer server = new KVServer(port, cacheSize, cacheStrategy);
		server.setServerName("Server "+args[0]);
		server.run();
	}

	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.persistenceLogic = new PersistenceLogic(cacheSize, strategy);
		this.acceptingRequests = false;
		this.writeLock = false;
		this.logger = Logger.getRootLogger();
	}

	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {
		running = initializeServer();
		stopped = false;
		if(serverSocket != null) {
			while(this.running) {
				try {
					listen();
				} catch (IOException e) {
					logger.error(serverName + ":Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info(serverName + ":Server stopped.");
	}

	private boolean initializeServer() {
		logger.info(serverName + ":Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info(serverName + ":Server listening on port: " + serverSocket.getLocalPort());    
			return true;
		} catch (IOException e) {
			logger.error(serverName + ":Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error(serverName + ":Port " + port + " is already bound!");
			}
			return false;
		}
	}

	private void listen() throws IOException {
		Socket client = serverSocket.accept();
		String messageHeader = getMessageHeader(client);	
		//Message Header is needed to proceed communication with ECS and Client in different ways. Can have values ECS or KV_MESSAGE
		if (messageHeader.equals(ConnectionType.ECS.toString())) {
			logger.info(serverName + ":Connection to ECS established");
			EcsConnection connection = new EcsConnection(client, this);
			new Thread(connection).start();
		} else if (messageHeader.equals(ConnectionType.KV_MESSAGE.toString())){
			ClientConnection connection = new ClientConnection(client, this);
			new Thread(connection).start();
		}	
		logger.info(serverName + ":Connected to " 
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
		logger.info(serverName + ":Checking input stream ...");
		while (input.available() > 0 && (symbol != (byte) 31)) {
			sb.append((char) symbol);
			symbol = (byte) input.read();
		}
		return sb.toString();
	}
	
	public void start() {
		acceptingRequests = true;
	}

	public void stop() {
		acceptingRequests = false;
	}

	public void shutDown() {
		
	//	moveData(new Range(), new Server());
		running = false;
	}

	public void lockWrite() {
		writeLock = true;
	}

	public void unLockWrite() {
		writeLock = false;
	}

	public void moveData(Range range, KVServerItem server) {
		/*		Socket socket = new Socket(server.getIp(), Integer.parseInt(server.getPort()));
		OutputStream output = socket.getOutputStream();
		output.write(ConnectionType.KV_MESSAGE.toString().getBytes());
		output.write((byte) 31);
		output.flush();
		for (String key: keys) {
			 if (range.in(HashGenerator.generateHashFor(key)) {
				 KVMessageItem getMessage = (KVMessageItem) persistenceLogic.get(key);
				 KVMessageItem messageToSend = new KVMessageItem(KvStatusType.PUT, key, getMessage.getValue());
				 KVMessageMarshaller marshaller = new KVMessageMarshaller();
				 output.write(marshaller.marshal(messageToSend));
			 }
		 }*/
	}

	public void updateStartIndex(byte[] startIndex) {
		this.startIndex = startIndex;
	}
	
	public PersistenceLogic getPersistenceLogic() {
		return persistenceLogic;
	}
	
	public boolean isAcceptingRequests() {
		return acceptingRequests;
	}

	public void setAcceptingRequests(boolean acceptingRequests) {
		this.acceptingRequests = acceptingRequests;
	}

	public void setStartIndex(byte[] startIndex) {
		logger.info(serverName + ":Server got start index " + Arrays.toString(startIndex));
		this.startIndex = startIndex;
	}

	public void setEndIndex(byte[] endIndex) {
		logger.info(serverName + ":Server got end index " + Arrays.toString(endIndex));
		this.endIndex = endIndex;
	} 
	
	public void setMetaDataTable(List<KVServerItem> metaDataTable) {
		this.metaDataTable = metaDataTable;
	}

	public byte[] getStartIndex() {
		return startIndex;
	}

	public byte[] getEndIndex() {
		return endIndex;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	
	public List<KVServerItem> getMetaDataTable() {
		return this.metaDataTable;
	}
	
}
