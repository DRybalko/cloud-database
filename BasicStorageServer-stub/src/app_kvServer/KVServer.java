package app_kvServer;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;
import app_kvServer.PersistenceLogic;
import app_kvServer.subscription.SubscriptionController;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.logic.KVServerItem;

/**
 * Main class for the KVServer. Holds the variables needed for
 * the connection with the server. It initializes and starts
 * the KVServer at given port and listens for clients until it is stopped.
 * 
 * @see PersistenceLogic
 */
public class KVServer {

	private ServerSocket serverSocket;
	private Logger logger;
	private PersistenceLogic persistenceLogic;
	private List<KVServerItem> metaDataTable;
	private ReplicaCoordinator replicaCoordinator;
	private FailureDetectorService failureDetector;
	private KVServerItem ecsMetaData;
	private ServerStatusInformation serverStatusInformation;
	private VersionController versionController;
	private SubscriptionController subscriptionController;

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
		new LogSetup("/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/logs/server/server"+args[0]+".log", Level.ALL);
		int port = Integer.parseInt(args[0]);
		int cacheSize = Integer.parseInt(args[1]);
		String cacheStrategy = args[2];
		String name = args[3];
		KVServer server = new KVServer(port, cacheSize, cacheStrategy, name);
		server.run();
	}

	public KVServer(int port, int cacheSize, String strategy, String name) {
		this.serverStatusInformation = new ServerStatusInformation(port, name);
		this.persistenceLogic = new PersistenceLogic(cacheSize, strategy);
		this.logger = Logger.getRootLogger();
		this.versionController = new VersionController(this);
		this.subscriptionController = new SubscriptionController();
	}

	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {
		serverStatusInformation.setRunning(initializeServer());
		if(serverSocket != null) {
			while(serverStatusInformation.isRunning()) {
				try {
					listen();
				} catch (IOException e) {
					logger.error(serverStatusInformation.getServerName() + ":Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info(serverStatusInformation.getServerName() + ":Server stopped.");
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(serverStatusInformation.getPort());
			logger.info("Server listening on port: " + serverSocket.getLocalPort());    
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + serverStatusInformation.getPort() + " is already bound!");
			}
			return false;
		}
	}

	private void listen() throws IOException {
		Socket client = serverSocket.accept();
		ClientConnection connection = new ClientConnection(client, this);
		new Thread(connection).start();
		logger.info(serverStatusInformation.getServerName() + ":Connected to " 
				+ client.getInetAddress().getHostName() 
				+  " on port " + client.getPort());
	}
	
	public void start() {
		serverStatusInformation.setAcceptingClientRequests(true);
		new Thread(failureDetector).start();
	}

	public void stop() {
		serverStatusInformation.setAcceptingClientRequests(false);
	}

	public void shutDown() {
		logger.info("Stopping server, closing socket...");
		this.replicaCoordinator.deleteAllReplications();
		DataTransferer dataTransferer = new DataTransferer(this, findNextServer());
		dataTransferer.transferAllKeys();
		try {		
			serverStatusInformation.setRunning(false);
			this.serverSocket.close();
		} catch (Exception e) {
			logger.info("Server does not accept requests anymore");
		}
		logger.info("Server shut down");
	}
	
	private KVServerItem findNextServer() {
		ListIterator<KVServerItem> iterator = metaDataTable.listIterator();
		while (iterator.hasNext()) {
			KVServerItem server = iterator.next();
			if (Arrays.equals(server.getStartIndex(), serverStatusInformation.getStartIndex()) 
					&& Arrays.equals(server.getEndIndex(), server.getEndIndex())) {
				if (iterator.hasNext()) return iterator.next();
				else return metaDataTable.get(0);
			}
		}
		return null;
	}

	public void lockWrite() {
		serverStatusInformation.setWriteLock(true);
	}

	public void unLockWrite() {
		serverStatusInformation.setWriteLock(false);
	}

	public void updateStartIndex(byte[] newStartIndex) {
		DataTransferer dataTransferer = new DataTransferer(this, findPreviousServer());
		dataTransferer.transfer( serverStatusInformation.getStartIndex(), newStartIndex);
		serverStatusInformation.setStartIndex(newStartIndex);
	}
	
	public KVServerItem findPreviousServer() {
		int thisServerIndex = metaDataTable.indexOf(serverStatusInformation.getThisKvServerItem());
		if (thisServerIndex == 0) return metaDataTable.get(metaDataTable.size() -1);
		else return metaDataTable.get(thisServerIndex - 1);
	} 
	
	public PersistenceLogic getPersistenceLogic() {
		return persistenceLogic;
	}

	public void setAcceptingRequests(boolean acceptingRequests) {
		serverStatusInformation.setAcceptingClientRequests(acceptingRequests);
	}

	public void setStartIndex(byte[] startIndex) {
		logger.info(serverStatusInformation.getServerName() + ":Server got start index " + Arrays.toString(startIndex));
		serverStatusInformation.setStartIndex(startIndex);
	}

	public void setEndIndex(byte[] endIndex) {
		logger.info(serverStatusInformation.getServerName() + ":Server got end index " + Arrays.toString(endIndex));
		serverStatusInformation.setEndIndex(endIndex);
	} 
	
	public void setMetaDataTable(List<KVServerItem> metaDataTable) {
		logger.debug("Received metadata table");
		this.metaDataTable = metaDataTable;
		serverStatusInformation.setThisKvServerItem(findServerInMetaDataTable());
		if (this.replicaCoordinator == null) this.replicaCoordinator = new ReplicaCoordinator(this);
		else this.replicaCoordinator.updateReplicas();
		if (this.failureDetector == null) this.failureDetector = new FailureDetectorService(this);
		else this.failureDetector.onMetaDataTableChange();
	}
	
	private KVServerItem findServerInMetaDataTable() {
		for (KVServerItem serverItem: metaDataTable) {
			if (serverItem.getName().equals(serverStatusInformation.getServerName()) && Arrays.equals(serverStatusInformation.getEndIndex(), serverItem.getEndIndex()))
				return serverItem;
		}
		return null;
	}

	public List<KVServerItem> getMetaDataTable() {
		return this.metaDataTable;
	}

	public ReplicaCoordinator getReplicaCoordinator() {
		return this.replicaCoordinator;
	}
	
	public ServerStatusInformation getServerStatusInformation() {
		return this.serverStatusInformation;
	}

	public KVServerItem getEcsMetaData() {
		return ecsMetaData;
	}
	
	public void setEcsMetaData(KVServerItem ecsMetaData) {
		this.ecsMetaData = ecsMetaData;
	}
	
	public VersionController getVersionController() {
		return versionController;
	}
	
	public SubscriptionController getSubscriptionController() {
		return this.subscriptionController;
	}

}