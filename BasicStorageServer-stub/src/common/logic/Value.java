package common.logic;

import java.time.LocalDateTime;

/**
 * This class represents value in our system. It has three class attributes: permission, time-stamp and value.
 * Permission defines, who can watch this value. Time-stamp is created, when user puts this value for the first
 * time. 
 *
 */
public class Value {

	private int permission;
	private LocalDateTime timestamp;
	private String value;
	
	public Value(int permission, LocalDateTime timestamp, String value) {
		this.permission = permission;
		this.timestamp = timestamp;
		this.value = value;
	}
	
	public Value(String value) {
		this.value = value;
	}
	
	public int getPermission() {
		return permission;
	}

	public void setPermission(int permission) {
		this.permission = permission;
	}

	public LocalDateTime getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(LocalDateTime timestamp) {
		this.timestamp = timestamp;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString(){
		String s  = "";
		s = this.permission + " " + this.timestamp + " " + this.value;
		return s; 
	 }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + permission;
		result = prime * result
				+ ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		Value other = (Value) obj;
		if (permission != other.permission)
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
}
