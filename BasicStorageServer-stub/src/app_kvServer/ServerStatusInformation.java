package app_kvServer;

import java.util.HashSet;
import java.util.Set;

import common.logic.KVServerItem;

public class ServerStatusInformation {

	private int port;
	private boolean running;
	private boolean writeLock;
	private boolean acceptingClientRequests;
	private byte[] startIndex;
	private byte[] endIndex;
	private String serverName;
	private Set<String> keys;
	private KVServerItem thisKvServerItem;
	
	public ServerStatusInformation(int port, String name) {
		this.acceptingClientRequests = false;
		this.writeLock = false;
		this.keys = new HashSet<>();
		this.port = port;
		this.serverName = name;
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
		keys.add(key);
	}
	
	public void removeKey(String key) {
		keys.remove(key);
	}
	
}
