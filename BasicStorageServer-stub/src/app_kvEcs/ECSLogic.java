package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Set;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.ECSMessage.EcsStatusType;
import common.messages.ECSMessageItem;
import common.messages.ECSMessageMarshaller;
import common.messages.MessageType;

public class ECSLogic {
	
	private Logger logger;
	private Repository repository;
	private MetaDataTableController metaDataTableController;

	public ECSLogic(Repository repository) {
		this.repository = repository;
		try {
			new LogSetup("/Users/dmitrij/git/cloud-database/BasicStorageServer-stub/logs/ecs/ecs.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger = Logger.getRootLogger();	
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
			metaDataTableController.moveFromAvailableToInitialized(metaDataTable.get(i));
		}
	}
	
	private String getServerConfiguration(String port, int cacheSize, String displacementStrategy) {
		return port + " " + cacheSize + " " +displacementStrategy;
	}
	
	public void start() throws IOException {
		Set<KVServerItem> initializedServers = metaDataTableController.getInitializedServers();
		for (KVServerItem server: initializedServers) {
			Socket socket = new Socket(server.getIp(), Integer.parseInt(server.getPort()));
			sendMessageVia(socket);
			InputStream input = socket.getInputStream();
			logger.info("Starting server with ip "+server.getIp() + ", port "+server.getPort());
			logger.info("Received message from server: " + readInputMessage(input));
			socket.close();
			metaDataTableController.moveFromInitializedToWorking(server);
		}
	}
	
	private void sendMessageVia(Socket socket) throws IOException {
		OutputStream output = socket.getOutputStream();
		output.write(MessageType.ECS.toString().getBytes());
		output.write((byte) 31);
		ECSMessageMarshaller marshaller = new ECSMessageMarshaller();
		output.write(marshaller.marshal(new ECSMessageItem(EcsStatusType.START)));
		output.flush();
	}
	
	private String readInputMessage(InputStream input) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		while (input.available() > 0) {
			stringBuilder.append((char) input.read());
		}
		return stringBuilder.toString();
	}
	
	public void stop() {
		//TODO add implementation
	}
	
	public void shutDown() {
		//TODO add implementation
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
		ecsLogic.initService(1, 10, "LFU");
		ecsLogic.start();
		System.exit(0);
	}
}
