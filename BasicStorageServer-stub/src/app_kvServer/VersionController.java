package app_kvServer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import common.logic.Value;

/**
 * This is the main class for version control for each key. In the current implementation system can persist up to
 * five version for the same key. In case there are more versions, the oldest ones are than deleted. 
 *
 */
public class VersionController {
	
	private final int MAX_VERSION_SIZE = 5;
	
	private KVServer server;
	
	//List for each key stores tuples of transformed key to store particular version of data
	//and time-stamp. The latest version has the highest index.
	private Map<String, PriorityBlockingQueue<KeyTimestampTuple>> keyVersionMap = new HashMap<>(); 
	
	public VersionController(KVServer server) {
		this.server = server;
	}
	
	public int getMaxVersionForKey(String key) {
		if (!keyVersionMap.containsKey(key)) return -1;
		return keyVersionMap.get(key).size();
	}
	
	public void removeKey(String key) {
		keyVersionMap.remove(key);
	}
	
	/**
	 * @param key
	 * @param timestamp
	 * @return generated key for given version to be used for persistence on disk
	 */
	public String addKey(String key, LocalDateTime timestamp) {
		if (keyVersionMap.containsKey(key)) {
			return addNewVersionToExistingKey(key, timestamp);
		} else {
			keyVersionMap.put(key, new PriorityBlockingQueue<KeyTimestampTuple>());
			return addNewKeyToKeyVersionList(key, timestamp);
		}
	}
	
	private String addNewVersionToExistingKey(String key, LocalDateTime timestamp) {
		PriorityBlockingQueue<KeyTimestampTuple> versionsForKeyList = keyVersionMap.get(key);
		if (versionsForKeyList.size() == MAX_VERSION_SIZE) deleteOldestVersion(key);
		return addNewKeyToKeyVersionList(key, timestamp);
	}

	private void deleteOldestVersion(String key) {
		String oldestVersionKey = keyVersionMap.get(key).poll().getKey();
		server.getPersistenceLogic().put(oldestVersionKey, new Value(0, "", null, "null"));
	}
	
	private String addNewKeyToKeyVersionList(String key, LocalDateTime timestamp) {
		ZonedDateTime zdt = timestamp.atZone(ZoneId.systemDefault());
		String keyWithVersion = key + zdt.toInstant().toEpochMilli();
		KeyTimestampTuple keyTimestamp = new KeyTimestampTuple(keyWithVersion, timestamp);
		keyVersionMap.get(key).add(keyTimestamp);
		return keyWithVersion;
	}
	
	public String getKeyForVersion(String key, int version) {
		if (!keyVersionMap.containsKey(key) || (version - 1) >= keyVersionMap.get(key).size()) return null;
		PriorityBlockingQueue<KeyTimestampTuple> keyQueue = keyVersionMap.get(key);
		return getVersion(keyQueue, version - 1).getKey();
	}
	
	private KeyTimestampTuple getVersion(PriorityBlockingQueue<KeyTimestampTuple> queue, int version) {
		KeyTimestampTuple[] queueArray = queue.toArray(new KeyTimestampTuple[0]);
		Arrays.sort(queueArray);
		return queueArray[version];
	}
	
	public void deleteKey(String key) {
		PriorityBlockingQueue<KeyTimestampTuple> versionsForKey = keyVersionMap.remove(key);
		if (versionsForKey == null) return;
		for (KeyTimestampTuple keyVersion: versionsForKey) {
			server.getPersistenceLogic().put(keyVersion.getKey(), new Value(0, "", null, "null"));
		}
	}
	
	public boolean hasKey(String key, LocalDateTime timestamp) {
		if (!this.keyVersionMap.containsKey(key)) return false;
		for (KeyTimestampTuple tuple: keyVersionMap.get(key)) {
			if (tuple.getTimestamp().equals(timestamp)) return true;
		}
		return false;
	}
	
}
