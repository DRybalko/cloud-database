package app_kvEcs.logic;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.logic.Communicator;
import common.logic.KVServerItem;
import common.logic.MetaDataTableController;
import common.messages.ECSMessage;
import common.messages.ECSMessage.EcsStatusType;
import common.messages.ConnectionType;
import common.messages.ECSMessageItem;
import common.messages.ECSMessageMarshaller;

public class ECSLogic {
	
	private final int MAX_WAITING_TIME = 10000;
	
	private Logger logger;
	private Repository repository;
	private MetaDataTableController metaDataTableController;
	private Communicator<ECSMessage> communicator;
	private ServerSetStatus serverSetStatus;

	public ECSLogic(Repository repository) {
		this.repository = repository;
		try {
			new LogSetup("/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/logs/ecs/ecs.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger = Logger.getRootLogger();	
		communicator = new Communicator<ECSMessage>(new ECSMessageMarshaller(), ConnectionType.ECS);
	}
	
	public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) {
		metaDataTableController = new MetaDataTableController(repository.getAvailableServers());
		serverSetStatus = new ServerSetStatus(repository.getAvailableServers());
		List<KVServerItem> metaDataTable = metaDataTableController.initializeTable(numberOfNodes);
		for (int i = 0; i < numberOfNodes; i++) {
			initializeServer(cacheSize, displacementStrategy, metaDataTable.get(i));
		}
		
	}
	
	private void initializeServer(int cacheSize, String displacementStrategy, KVServerItem server) {
		/*String[] cmd = {"ssh", "-n", "localhost", "java", "-jar", "/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/ms3-server.jar",
				getServerConfiguration(server.getPort(), cacheSize, displacementStrategy, server.getName())};		
		try {
			Runtime.getRuntime().exec(cmd);
		} catch (IOException e) {
			logger.error("Server with IP: " + server.getIp() + " and Port: " + server.getPort() + " could not be launched."+e.getMessage());
		} */
		if (isInitialized(server)) {
			serverSetStatus.moveFromAvailableToInitialized(server);
			sendIndices(server);
			sendMetaDataTable(server);
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

	private String getServerConfiguration(String port, int cacheSize, String displacementStrategy, String name) {
		return port + " " + cacheSize + " " +displacementStrategy+" "+name;
	}
		
	private void sendIndices(KVServerItem server) {
		ECSMessageItem indicesMessage = new ECSMessageItem(EcsStatusType.SERVER_START_END_INDEX, server.getStartIndex(), server.getEndIndex());
		logger.info("Try to send indices to server with name: "+server.getName());
		logger.debug("Start index: " + Arrays.toString(indicesMessage.getStartIndex()));
		logger.debug("End index: " + Arrays.toString(indicesMessage.getEndIndex()));
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, indicesMessage);
		logger.info("Server "+server.getName() +" replied: "+reply.getStatus().toString());
	}
	
	private void sendMetaDataTable(KVServerItem server) {
		ECSMessageItem metaDataTableMessage = new ECSMessageItem(EcsStatusType.META_DATA_TABLE, metaDataTableController.getMetaDataTable());
		logger.info("Try to send metaDataTable to server with name: "+server.getName());
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, metaDataTableMessage);
		logger.info("Server "+server.getName() +" replied: "+reply.getStatus().toString());
	}
	
	public void start() {
		List<KVServerItem> initializedServers = serverSetStatus.getInitializedServers();
		for (KVServerItem server: initializedServers) {
			startServer(server);
		}
		serverSetStatus.moveAllFromInitializedToWorking();
	}
	
	private void startServer(KVServerItem server) {
		ECSMessageItem message = new ECSMessageItem(EcsStatusType.START);
		logger.info("Try to start server with name: "+server.getName()+", ip: "+server.getIp()+", port: "+server.getPort());
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
		logger.info("Server replied with: " + reply.getStatus().toString());
	}
	
	public void stop() {
		List<KVServerItem> workingServers = serverSetStatus.getWorkingServers();
		for (KVServerItem server: workingServers) {
			ECSMessageItem message = new ECSMessageItem(EcsStatusType.STOP);
			ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
			logger.info("Stopping server "+server.getName()+". Reply: "+reply.getStatus().toString());
		}
		serverSetStatus.moveAllFromWorkingToInitialized();
	}
	
	public void shutDown() {
		List<KVServerItem> initializedServers = serverSetStatus.getInitializedServers();
		for (KVServerItem server: initializedServers) {
			shutDownServer(server);
		}
		serverSetStatus.moveAllFromInitializedToAvailable();
	}
	
	private void shutDownServer(KVServerItem server) {
		ECSMessageItem message = new ECSMessageItem(EcsStatusType.SHUT_DOWN);
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(server, message);
		logger.info("Shutting server "+server.getName()+" down. Reply: "+reply.getStatus().toString());
	}
	
	public void addNode(int cacheSize, String displacementStrategy) {
		KVServerItem newServer = serverSetStatus.getAvailableServers().get(0);
		KVServerItem serverToUpdate = metaDataTableController.addServerToMetaData(newServer);
		initializeServer(cacheSize, displacementStrategy, newServer);
		serverSetStatus.moveFromAvailableToInitialized(newServer);
		startServer(newServer);
		serverSetStatus.moveFromInitializedToWorking(newServer);
		updateStartIndex(serverToUpdate, newServer);
		updateMetaDataTableOfWorkingServers();
	}
	
	private void updateMetaDataTableOfWorkingServers() {
		for (KVServerItem server: serverSetStatus.getWorkingServers()) {
			sendMetaDataTable(server);
		}
	}

	private void updateStartIndex(KVServerItem dataTransferFrom, KVServerItem dataTransferTo) {
		ECSMessageItem message = new ECSMessageItem(EcsStatusType.UPDATE_START_INDEX, dataTransferFrom.getStartIndex());
		logger.info("Try to update start index of server with name: "+dataTransferFrom.getName()+", ip: "+dataTransferFrom.getIp()+", port: "+dataTransferFrom.getPort());
		ECSMessageItem reply = (ECSMessageItem) communicator.sendMessage(dataTransferFrom, message);
		logger.info("Server to update replied with: " + reply.getStatus().toString());
		ECSMessageItem transferDataReply = (ECSMessageItem) communicator.sendMessage(dataTransferTo, reply);
		logger.info("New server replied with: " + transferDataReply.getStatus().toString());
	}
	
	public void removeNode() {
		KVServerItem serverToRemove;
		if (serverSetStatus.getWorkingServers().size() > 0) {
			serverToRemove = serverSetStatus.getWorkingServers().get(0);
			removeServer(serverToRemove);
		} else if (serverSetStatus.getInitializedServers().size() > 0) {
			serverToRemove = serverSetStatus.getInitializedServers().get(0);
			removeServer(serverToRemove);
		} else {
			logger.warn("Server could not be removed. No servers to remove");
		}

	}
	
	private void removeServer(KVServerItem serverToRemove) {
		KVServerItem serverToTransferData = metaDataTableController.removeServerFromMetaData(serverToRemove);
		serverToRemove.setStartIndex(serverToRemove.getEndIndex());
		updateStartIndex(serverToRemove, serverToTransferData);
		ECSMessage stopMessage = new ECSMessageItem(EcsStatusType.SHUT_DOWN);
		communicator.sendMessage(serverToRemove, stopMessage);
		serverSetStatus.moveFromWorkingToInitialized(serverToRemove);
		serverSetStatus.moveFromInitializedToAvailable(serverToRemove);
		updateMetaDataTableOfWorkingServers();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Repository repository = new Repository("ecs.config");
		ECSLogic ecsLogic = new ECSLogic(repository);
		ecsLogic.initService(2, 10, "LFU");
		ecsLogic.start();
	//	ecsLogic.addNode(10, "LFU");
		ecsLogic.removeNode();
	}
}
