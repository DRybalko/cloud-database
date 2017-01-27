package common.messages.serverToServerMessage;

import common.messages.Message;

public class ServerToServerMessageItem extends Message implements ServerToServerMessage {
	
	private ServerToServerStatusType status;
	private String key;
	private String port;
	private String ip;
	
	public ServerToServerMessageItem(ServerToServerStatusType status) {
		this.status = status;
	}
	
	public ServerToServerMessageItem(ServerToServerStatusType status, String key, String port, String ip) {
		this.status = status;
		this.port = port;
		this.key = key;
		this.ip = ip;
	}
	
	public ServerToServerStatusType getStatus() {
		return this.status;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getPort() {
		return this.port;
	}
	
	public void setPort(String port) {
		this.port = port;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getIp() {
		return ip;
	}
}
