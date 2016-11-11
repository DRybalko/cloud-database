package app_kvEcs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class ECSLogic {
	
	private Logger logger;
	private Repository repository;
	private List<KVServerItem> metaDataTable;
	
	public ECSLogic(Repository repository) {
		this.repository = repository;
	}
	
	public void initService(int numberOfNodes, int cacheSize, String desplacementStrategy) {
		MetaDataTableInitializer tableInitializer = new MetaDataTableInitializer(repository.getAvailableServers());
		metaDataTable = tableInitializer.initializeTable(numberOfNodes);
		startServer();
	}
	
	private void startServer() {
		Process process;
		Runtime run = Runtime.getRuntime();
		String command = "ssh -n localhost java -jar /Users/dmitrij/git/cloud-database/BasicStorageServer-stub/ms3-server.jar 50000 ERROR";
		try {
			process = run.exec(command);
			InputStream ir = process.getInputStream();
			byte[] input = new byte[100];
			char[] show = new char[100];
			ir.read(input);
			for (int i=0; i<100; i++) {
				show[i] = (char) input[i];
			}
			System.out.println(Arrays.toString(show));
		} catch (IOException e) {
			e.getStackTrace();
		//	logger.error("Server could not be launched."+e.getMessage());
		}
	}
	
	private void startServers() {
		Process process;
		Runtime run = Runtime.getRuntime();
		for (KVServerItem server: metaDataTable) {
			String command = "ssh -n "+server.getIp()+" nohup java -jar /Users/dmitrij/git/cloud-database/BasicStorageServer-stub/ms3-server.jar +" + server.getPort() + " ERROR &";
			try {
				process = run.exec(command);
			} catch (IOException e) {
				logger.error("Server could not be launched."+e.getMessage());
			}
		}
	}
	
	public void start() {
		
	}
	
	public void stop() {
		
	}
	
	public void shutDown() {
		
	}
	
	public void addNode(int cacheSize, String displacementStrategy) {
		
	}
	
	public void removeNode() {
		
	}
	
	public static void main(String[] args) {
		Repository repository = new Repository("ecs.config");
		ECSLogic ecsLogic = new ECSLogic(repository);
		ecsLogic.initService(1, 10, "LFU");
	}
	
}
