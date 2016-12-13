package app_kvServer;

import java.util.LinkedList;
import java.util.List;

import common.logic.KVServerItem;

public class ServerSetStatus {

	private List<KVServerItem> availableServers;
	private List<KVServerItem> workingServers;
	private List<KVServerItem> initializedServers;
	
	public ServerSetStatus(List<KVServerItem> availableServers) {
		this.availableServers = availableServers;
		this.workingServers = new LinkedList<>();
		this.initializedServers = new LinkedList<>();	
	}
	
	//Needed to avoid ConcurrentModificationException
	public void moveAllFromAvailableToInitialized() {
		this.initializedServers.addAll(availableServers);
		availableServers = new LinkedList<>();
	}
	
	public void moveFromAvailableToInitialized(KVServerItem server) {
		this.availableServers.remove(server);
		this.initializedServers.add(server);
	}
	
	//Needed to avoid ConcurrentModificationException
	public void moveAllFromWorkingToInitialized() {
		this.initializedServers.addAll(workingServers);
		this.workingServers = new LinkedList<>();
	}
	public void moveFromWorkingToInitialized(KVServerItem server) {
		this.workingServers.remove(server);
		this.initializedServers.add(server);
	}
	
	//Needed to avoid ConcurrentModificationException
	public void moveAllFromInitializedToWorking() {
		this.workingServers.addAll(initializedServers);
		this.initializedServers = new LinkedList<>();
	}
	
	public void moveFromInitializedToWorking(KVServerItem server) {
		this.initializedServers.remove(server);
		this.workingServers.add(server);
	}
	
	//Needed to avoid ConcurrentModificationException
	public void moveAllFromInitializedToAvailable() {
		this.availableServers.addAll(initializedServers);
		this.initializedServers = new LinkedList<>();
	}
	
	public void moveFromInitializedToAvailable(KVServerItem server) {
		this.initializedServers.remove(server);
		this.availableServers.add(server);
	}
	
	public List<KVServerItem> getAvailableServers() {
		return this.availableServers;
	}
	
	public List<KVServerItem> getWorkingServers() {
		return this.workingServers;
	}
	
	public List<KVServerItem> getInitializedServers() {
		return this.initializedServers;
	}
	
}
