package app_kvEcs;

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
	}
	
}
