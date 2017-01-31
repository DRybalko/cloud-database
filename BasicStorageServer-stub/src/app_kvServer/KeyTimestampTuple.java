package app_kvServer;

import java.time.LocalDateTime;

/**
 * This class is used to create versions of one key. It contains information about the key, which is used in cache/on disk
 * to store data. It also contains time-stamp, when key was created.
 *
 */
public class KeyTimestampTuple implements Comparable<KeyTimestampTuple> {

	private String key;
	private LocalDateTime timestamp;

	public KeyTimestampTuple(String key, LocalDateTime timestamp) {
		this.key = key;
		this.timestamp = timestamp;
	}

	public String getKey() {
		return key;
	}

	public LocalDateTime getTimestamp() {
		return timestamp;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public void setTimestamp(LocalDateTime timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public int compareTo(KeyTimestampTuple tuple) {
		return this.getTimestamp().compareTo(tuple.getTimestamp());
	}

}
