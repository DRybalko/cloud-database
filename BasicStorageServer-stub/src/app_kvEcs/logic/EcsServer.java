package app_kvEcs.logic;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

public class EcsServer implements Runnable {

	private static final int PORT = 60000;
	
	private boolean running;
	private ServerSocket serverSocket;
	private Logger logger;
	private ECSLogic ecsLogic;
	
	public EcsServer(ECSLogic ecsLogic) {
		this.ecsLogic = ecsLogic;
		logger = Logger.getRootLogger();
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
			serverSocket = new ServerSocket(PORT);
			logger.info("Server listening on port: " + serverSocket.getLocalPort());    
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + PORT + " is already bound!");
			}
			return false;
		}
	}

	private void listen() throws IOException {
		Socket client = serverSocket.accept();
		FaultyServerProcessor connection = new FaultyServerProcessor(client, ecsLogic);
		connection.processConnection();
	}

	public boolean isRunning() {
		return this.running;
	}
	
}
