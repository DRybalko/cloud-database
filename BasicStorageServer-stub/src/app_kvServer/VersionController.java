package app_kvServer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private Map<String, List<KeyTimestampTuple>> keyVersionMap = new HashMap<>(); 
	
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
			keyVersionMap.put(key, new ArrayList<>());
			return addNewKeyToKeyVersionList(key, timestamp);
		}
	}
	
	private String addNewVersionToExistingKey(String key, LocalDateTime timestamp) {
		List<KeyTimestampTuple> versionsForKeyList = keyVersionMap.get(key);
		if (versionsForKeyList.size() == MAX_VERSION_SIZE) deleteOldestVersion(key);
		return addNewKeyToKeyVersionList(key, timestamp);
	}

	private void deleteOldestVersion(String key) {
		String oldestVersionKey = keyVersionMap.get(key).remove(0).getKey();
		server.getPersistenceLogic().put(oldestVersionKey, new Value(0, null, "null"));
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
		return keyVersionMap.get(key).get(version - 1).getKey();
	}
	
	public void deleteKey(String key) {
		List<KeyTimestampTuple> versionsForKey = keyVersionMap.remove(key);
		if (versionsForKey == null) return;
		for (KeyTimestampTuple keyVersion: versionsForKey) {
			server.getPersistenceLogic().put(keyVersion.getKey(), new Value(0, null, "null"));
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
