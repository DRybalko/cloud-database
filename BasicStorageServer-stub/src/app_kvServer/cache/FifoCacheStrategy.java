package app_kvServer.cache;

import java.util.HashMap;
import java.util.LinkedList;

import app_kvServer.KVTuple;

public class FifoCacheStrategy implements CacheStrategy {

	private LinkedList<KVTuple> queue;
	private HashMap<String, String> kvPairs;
	
	public FifoCacheStrategy() {
		queue = new LinkedList<>();
		kvPairs = new HashMap<>();		
	}

	public void addElement(String key, String value) {
		kvPairs.put(key, value);
		queue.add(new KVTuple(key, value));
	}
	
	public KVTuple deleteElement() {
		KVTuple tuple = queue.removeFirst();
		kvPairs.remove(tuple.getKey());
		return tuple;
	}

	public String getValueFor(String key) {
		return kvPairs.get(key);
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, String value) {
		kvPairs.replace(key, value);
	}

	public int size() {
		return kvPairs.size();
	}
	
	public void deleteValueFor(String key) {
		String value = kvPairs.remove(key);
		queue.remove(new KVTuple(key, value));
	}
	
}