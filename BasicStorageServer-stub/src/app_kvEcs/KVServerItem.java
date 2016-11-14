package app_kvEcs;

public class KVServerItem {
	
	private String name;
	private String ip;
	private String port;
	private byte[] startIndex;
	private byte[] endIndex;

	public KVServerItem(String name, String ip, String port) {
		this.name = name;
		this.ip = ip;
		this.port = port;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}


	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
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
}
