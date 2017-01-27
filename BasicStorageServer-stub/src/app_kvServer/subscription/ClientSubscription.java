package app_kvServer.subscription;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * This class contains information about the client, that subscribed to key.
 *
 */
public class ClientSubscription {
	
	private String ip;
	private String port;
	
	public ClientSubscription(SocketAddress socketAddress, String port) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
		InetAddress inetAddress = inetSocketAddress.getAddress();
		this.ip = inetAddress.getHostAddress();
		this.port = port;
	}
	
	public ClientSubscription(String ip, String port) {
		this.ip = ip;
		this.port = port;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + ((port == null) ? 0 : port.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClientSubscription other = (ClientSubscription) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (port == null) {
			if (other.port != null)
				return false;
		} else if (!port.equals(other.port))
			return false;
		return true;
	}
	
	
	
}
