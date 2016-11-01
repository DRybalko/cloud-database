package app_kvServer;

import java.io.IOException;
import java.net.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer extends Thread {
	
	private static final int MAX_CACHE_SIZE = 3;
	private static final String CACHE_STRATEGY = "lfu";

	private ServerSocket serverSocket;
	private static Logger logger = Logger.getRootLogger();
	private int port;
	private boolean running;
	private PersistenceLogic persistenceLogic;
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 * @throws IOException 
	 */
	public KVServer(int port, int cacheSize, String cacheStrategy) {
		this.port = port;
		this.persistenceLogic = new PersistenceLogic(cacheSize, cacheStrategy);
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

	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		setupLogger();
		try {
			processArgs(args);
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
	
	private static void processArgs(String[] args) throws IOException {
		if(args.length != 1) {
			System.out.println("Error! Invalid number of arguments!");
			System.out.println("Usage: Server <port>!");
		} else {
			int port = Integer.parseInt(args[0]);
			new KVServer(port, MAX_CACHE_SIZE, CACHE_STRATEGY).start();
		}
	}
	
	private static void setupLogger() {
		try {
			new LogSetup("logs/server/server.log", Level.ALL);
		} catch (IOException e) {
			System.out.println("Logger could not be initialized");
		}
	}
}
