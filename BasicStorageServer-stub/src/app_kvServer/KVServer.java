package app_kvServer;

import java.io.IOException;
import java.net.*;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;
import app_kvServer.PersistenceLogic;
import logger.LogSetup;
import common.messages.KVMessage;
import common.messages.KVMessageItem;
import app_kvServer.Metadata;
import app_kvServer.Range;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Main class for the KVServer. Holds the variables needed for
 * the connection with the server (like the port number, the 
 * cache size and the cache strategy). It initializes and starts
 * the KVServer at given port and listens for clients until it is stopped.
 * 
 * @see PersistenceLogic
 */


public class KVServer extends Thread {
	

	private static final int MAX_CACHE_SIZE = 50;
	private static final String CACHE_STRATEGY = "lfu";
	
	private int cacheSize;
	private ServerSocket serverSocket;
	private Logger logger;
	private int port;
	private boolean running;
	private PersistenceLogic persistenceLogic;
	private boolean writeLock;
	private Metadata metadata;
	private String key;
    private boolean acceptingRequests;
    private KVServer server;
    private String displacementStrategy;
    private KVMessageItem kvMessage;
    private ClientConnection client;
    
    /**
     * Initialize the KVServer with the meta-data, it’s local cache size,
     * and the cache displacement strategy, and block it for client 
     * requests, i.e., all client requests are rejected with an 
     * SERVER_STOPPED error message; ECS requests have to be processed.
     * 
     * @param metadata
     * @param cacheSize
     * @param displacementStrategy
     */
    
    public void initKVServer(Metadata metadata, int cacheSize, String displacementStrategy){
    	server.setMetadata(metadata);
    	server.setCacheSize(cacheSize);
    	server.setStratedy(displacementStrategy);
    	//block for client requests
    	stopS();
    	kvMessage.setStatus(KVMessage.StatusType.SERVER_STOPPED);
    	try {
			client.sendMessage(kvMessage);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    /**
     * Starts the KVServer, all client requests and all ECS requests are processed.
     */
    
    public void start(){
    	acceptingRequests = true;
    	
    }

	/**
     * Stops the KVServer, all client requests are rejected and only 
     * ECS requests are processed.
     */
    
    public void stopS(){
    	acceptingRequests = false;
    }
    
    /**
     * Exits the KVServer application.
     */
    
    public void shutDown(){
    	server.shutDown();
    }
    
    /**
     * Lock the KVServer for write operations.
     */
    
    public void lockWrite(){
    	server.setLock(true);
    }
    
    /**
     * Unlock the KVServer for write operations.
     */
    
    public void unLockWrite(){
    	server.setLock(false);
    }
    
    /**
     * Transfer a subset (range) of the KVServer’s data to another 
     * KVServer (reallocation before removing this server or adding a 
     * new KVServer to the ring); send a notification to the ECS, if data 
     * transfer is completed.
     * 
     * @param range
     * @param server
     */
    
    public void moveData(Range range, KVServer server){
    	
    }
    
    /**
     * Update the meta-data repository of this server
     * 
     * @param metadata
     */
    
    public void update(Metadata metadata){
    	stopS();
    	server.setMetadata(metadata);
    	start();
    } 
    
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
		public KVServer(int port, int cacheSize, String strategy) {
			this.port = port;
			this.persistenceLogic = new PersistenceLogic(cacheSize, strategy);
			this.acceptingRequests = false;
			this.writeLock = false;
		}
		
		/**
		 * Initializes and starts the server. 
		 * Loops until the the server should be closed.
		 */
		public void run() {
			running = initializeServer();		
			if(serverSocket != null) {
				while(isRunning()){
					try {
						listen();
					} catch (IOException e) {
						logger.error("Error! " + "Unable to establish connection. \n", e);
					}
				}
			}
			logger.info("Server stopped.");
		}
		
		private boolean isRunning() {
			return this.running;
		}
		
		private void listen() throws IOException {
			Socket client = serverSocket.accept();                
			ClientConnection connection = new ClientConnection(client, persistenceLogic);
			new Thread(connection).start();

			logger.info("Connected to " 
					+ client.getInetAddress().getHostName() 
					+  " on port " + client.getPort());
		}

		
		private boolean initializeServer() {
			logger.info("Initialize server ...");
			try {
				serverSocket = new ServerSocket(port);
				logger.info("Server listening on port: " + serverSocket.getLocalPort());    
				return true;

			} catch (IOException e) {
				logger.error("Error! Cannot open server socket:");
				if(e instanceof BindException){
					logger.error("Port " + port + " is already bound!");
				}
				return false;
			}
		}
		/**
		 * Stops the server insofar that it won't listen at the given port any more.
		 */
		public void stopServer(){
			running = false;
			try {
				serverSocket.close();
			} catch (IOException e) {
				logger.error("Error! " +
						"Unable to close socket on port: " + port, e);
			}
		}
		
		public String getKey(){
			return key;
		}
		
		public void setKey(String key){
			this.key = key;
		}
		
		public void setMetadata(Metadata metadata){
			this.metadata = metadata;
		}
		
		public Metadata getMetadata(){
			return metadata;
		}
		
		public boolean isLocked() {
			return writeLock;
		}

		public void setLock(boolean writeLock) {
			this.writeLock = writeLock;
		}
		
		public void setCacheSize(int cacheSize){
			this.cacheSize = cacheSize;
		}
		
		public int getCacheSize(){
			return cacheSize;
		}
		
		public void setStratedy(String displacementStrategy){
			this.displacementStrategy = displacementStrategy;
		}
		
		public String getStrategy(){
			return displacementStrategy;
		}
		
	}
