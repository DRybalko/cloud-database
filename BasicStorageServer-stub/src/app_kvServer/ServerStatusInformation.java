package app_kvServer;

import java.util.HashSet;
import java.util.Set;

import common.logic.KVServerItem;

/**
 * This class contains all the technical information about the server. It has no logic, but only getters and setters
 * for different attributes of the server.
 */
public class ServerStatusInformation {

	private int port;
	private boolean running;
	private boolean writeLock;
	private boolean acceptingClientRequests;
	private byte[] startIndex;
	private byte[] endIndex;
	private String serverName;
	private Set<String> keys;
	private Set<String> replicationKeys;
	private KVServerItem thisKvServerItem;
	
	public ServerStatusInformation(int port, String name) {
		this.acceptingClientRequests = false;
		this.writeLock = false;
		this.keys = new HashSet<>();
		this.port = port;
		this.serverName = name;
		this.replicationKeys = new HashSet<>();
	}
	
	public void setRunning(boolean running) {
		this.running = running;
	}
	
	public boolean isRunning() {
		return this.running;
	}
	
	public Set<String> getKeys() {
		return keys;
	}
	
	public byte[] getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(byte[] startIndex) {
		this.startIndex = startIndex;
	}
	
	public byte[] getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(byte[] endIndex) {
		this.endIndex = endIndex;
	}
	
	public String getServerName() {
		return serverName;
	}

	public KVServerItem getThisKvServerItem() {
		return this.thisKvServerItem;
	}
	
	public void setThisKvServerItem(KVServerItem serverItem) {
		this.thisKvServerItem = serverItem;
		this.startIndex = serverItem.getStartIndex();
		this.endIndex = serverItem.getEndIndex();
	}
	
	public boolean isAcceptingClientRequests() {
		return acceptingClientRequests;
	}
	
	public void setAcceptingClientRequests(boolean acceptingClientRequests) {
		this.acceptingClientRequests = acceptingClientRequests;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setWriteLock(boolean lock) {
		this.writeLock = lock;
	}
	
	public boolean getWriteLock() {
		return writeLock;
	}
	
	public void addKey(String key) {
		if (!keys.contains(key)) keys.add(key);
	}
	
	public void removeKey(String key) {
		keys.remove(key);
	}
	
	public void addReplicationKey(String replicationKey) {
		replicationKeys.add(replicationKey);
	}
	
	public void removeReplicationKey(String replicationKey) {
		replicationKeys.remove(replicationKey);
	}
	
	public Set<String> getReplicationKeys() {
		return replicationKeys;
	}
}