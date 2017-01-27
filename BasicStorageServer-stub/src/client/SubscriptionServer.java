package client;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

import org.apache.log4j.Logger;

/**
 * In order to get notification messages from server, when key was changed or deleted, 
 * client has to listen to possible connection from the server.
 *
 */
public class SubscriptionServer implements Runnable {
	
	private final int MIN_PORT = 40000;
	private final int MAX_PORT = 49999;
	
	private int port;
	private boolean running;
	private ServerSocket serverSocket;
	private Logger logger;
	private KVStore kvStore;
	
	public SubscriptionServer(KVStore kvStore) {
		this.port = randInt(MIN_PORT, MAX_PORT);
		this.logger = Logger.getRootLogger();
		this.kvStore = kvStore;
	}
	
	public static int randInt(int min, int max) {
	    Random r = new Random();
	    return r.nextInt((max - min) + 1) + min;
	}
	
	public void run() {
		running = initializeServer();
		if(serverSocket != null) {
			while(this.running) {
				try {
					listen();
				} catch (IOException e) {
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
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

	private void listen() throws IOException {
		Socket client = serverSocket.accept();
		NotificationMessageProcessor notificationMessageProcessor = new NotificationMessageProcessor(client, kvStore);
		new Thread(notificationMessageProcessor).start();
	}

	public boolean isRunning() {
		return this.running;
	}
	
	public int getPort() {
		return port;
	}
	
}
