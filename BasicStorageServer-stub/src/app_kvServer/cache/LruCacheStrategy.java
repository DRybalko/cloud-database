package app_kvServer.cache;

import java.util.HashMap;
import java.util.LinkedList;

import app_kvServer.KVTuple;

public class LruCacheStrategy implements CacheStrategy {

	private LinkedList<KVTuple> queue;
	private HashMap<String, String> kvPairs;
	
	public LruCacheStrategy() {
		queue = new LinkedList<>();
		kvPairs = new HashMap<>();
	}
	public void addElement(String key, String value) {
		kvPairs.put(key, value);
		queue.addFirst(new KVTuple(key, value));
	}

	public String getValueFor(String key) {
		KVTuple tuple = new KVTuple(key, kvPairs.get(key));
		queue.remove(tuple);	
		queue.addFirst(tuple);
		return tuple.getValue();
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, String value) {
		KVTuple oldTuple = new KVTuple(key, kvPairs.get(key));
		kvPairs.replace(key, value);
		queue.remove(oldTuple);
		queue.addFirst(new KVTuple(key, value));
	}

	public KVTuple deleteElement() {
		KVTuple tuple = queue.pollLast();
		kvPairs.remove(tuple.getKey());
		return tuple;
	}

	public int size() {
		return kvPairs.size();
	}

	public void deleteValueFor(String key) {
		String value = kvPairs.remove(key);
		queue.remove(new KVTuple(key, value));
	}
}
