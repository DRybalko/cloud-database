package app_kvEcs;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.ECSMessage.EcsStatusType;
import common.messages.ECSMessageItem;

public class ECSLogic {
	
	private final int MAX_WAITING_TIME = 10000;
	
	private Logger logger;
	private Repository repository;
	private MetaDataTableController metaDataTableController;
	private Communicator communicator;

	public ECSLogic(Repository repository) {
		this.repository = repository;
		try {
			new LogSetup("/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/logs/ecs/ecs.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger = Logger.getRootLogger();	
		communicator = new Communicator();
	}
	
	public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) {
		metaDataTableController = new MetaDataTableController(repository.getAvailableServers());
		initializeServers(numberOfNodes, cacheSize, displacementStrategy);
		
	}
	
	private void initializeServers(int numberOfNodes, int cacheSize, String displacementStrategy) {
		List<KVServerItem> metaDataTable = metaDataTableController.initializeTable(numberOfNodes);
		for (int i = 0; i < numberOfNodes; i++) {
			String[] cmd = {"ssh", "-n", "localhost", "java", "-jar", "/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/ms3-server.jar",
					getServerConfiguration(metaDataTable.get(i).getPort(), cacheSize, displacementStrategy)};		
			try {
				Runtime.getRuntime().exec(cmd);
			} catch (IOException e) {
				logger.error("Server with IP: " + metaDataTable.get(i).getIp() + " and Port: " + metaDataTable.get(i).getPort() + " could not be launched."+e.getMessage());
			}
			if (isInitialized(metaDataTable.get(i))) {
				metaDataTableController.moveOneFromAvailableToInitialized(metaDataTable.get(i));
				try {
					sendIndicesAndMetaData(metaDataTable.get(i));
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}
	
	private boolean isInitialized(KVServerItem server) {
		int waitingTime = 0;
		while (!communicator.checkStarted(server)) {
			try {
				Thread.sleep(500);
				waitingTime += 500;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (waitingTime > MAX_WAITING_TIME) {
				logger.error("Server " + server.getName() + " could not be started!");
				return false;
			}
		};
		return true;
	}

	private String getServerConfiguration(String port, int cacheSize, String displacementStrategy) {
		return port + " " + cacheSize + " " +displacementStrategy;
	}
	
	// TODO Can currently send meta data table with max 4 elements. More then 4 elements cause exception
	private void sendIndicesAndMetaData(KVServerItem server) throws IOException {
		ECSMessageItem indicesMessage = new ECSMessageItem(EcsStatusType.SERVER_START_END_INDEX, server.getStartIndex(), server.getEndIndex());
		logger.info("Try to send indices to server with name: "+server.getName());
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, indicesMessage);
		logger.info("Server "+server.getName() +" replied: "+reply.getStatus().toString());
		ECSMessageItem metaDataTableMessage = new ECSMessageItem(EcsStatusType.META_DATA_TABLE, metaDataTableController.getMetaDataTable());
		logger.info("Try to send metaDataTable to server with name: "+server.getName());
		communicator.sendMessage(server, metaDataTableMessage);
	}
	
	public void start() {
		Set<KVServerItem> initializedServers = metaDataTableController.getInitializedServers();
		for (KVServerItem server: initializedServers) {
			ECSMessageItem message = new ECSMessageItem(EcsStatusType.START);
			logger.info("Try to start server with name: "+server.getName()+", ip: "+server.getIp()+", port: "+server.getPort());
			ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
			logger.info("Server replied with: " + reply.getStatus().toString());
		}
		metaDataTableController.moveFromInitializedToWorking();
	}
	
	public void stop() {
		Set<KVServerItem> workingServers = metaDataTableController.getWorkingServers();
		for (KVServerItem server: workingServers) {
			ECSMessageItem message = new ECSMessageItem(EcsStatusType.STOP);
			ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
			logger.info("Stopping server "+server.getName()+". Reply: "+reply.getStatus().toString());
			metaDataTableController.moveFromWorkingToInitialized();
		}
	}
	
	public void shutDown() {
		Set<KVServerItem> initializedServers = metaDataTableController.getInitializedServers();
		for (KVServerItem server: initializedServers) {
			ECSMessageItem message = new ECSMessageItem(EcsStatusType.SHUT_DOWN);
			ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
			logger.info("Shutting server "+server.getName()+" down. Reply: "+reply.getStatus().toString());
			metaDataTableController.moveFromInitializedToAvailable();
		}
	}
	
	public void addNode(int cacheSize, String displacementStrategy) {
		//TODO add implementation
	}
	
	public void removeNode() {
		//TODO add implementation
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Repository repository = new Repository("ecs.config");
		ECSLogic ecsLogic = new ECSLogic(repository);
		ecsLogic.initService(4, 10, "LFU");
		ecsLogic.start();
		System.exit(0);
	}
}
