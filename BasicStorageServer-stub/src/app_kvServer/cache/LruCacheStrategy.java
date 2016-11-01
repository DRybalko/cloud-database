package app_kvServer.cache;

import java.util.HashMap;
import java.util.LinkedList;

import app_kvServer.KVTuple;

/**
 * That is the implementation of the LRU cache strategy.
 * The class provides methods for insertion/deletion
 * of a <key>,<value> tuple, finding/deletion a value of a key,
 * and other, that work on the least recently used principle.
 *
 * @see CacheStrategy
 */


public class LruCacheStrategy implements CacheStrategy {

	private LinkedList<KVTuple> queue;
	private HashMap<String, KVTuple> kvPairs;
	
	public LruCacheStrategy() {
		queue = new LinkedList<>();
		kvPairs = new HashMap<>();
	}
	public void addElement(String key, String value) {
		KVTuple tupleToSave = new KVTuple(key, value);
		kvPairs.put(key, tupleToSave);
		queue.addFirst(tupleToSave);
	}

	public String getValueFor(String key) {
		KVTuple foundKvTuple = kvPairs.get(key);
		queue.remove(foundKvTuple);	
		queue.addFirst(foundKvTuple);
		return foundKvTuple.getValue();
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, String value) {
		KVTuple tuple = kvPairs.get(key);
		tuple.setValue(value);
		queue.remove(tuple);
		queue.addFirst(tuple);
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
		KVTuple tupleToDelete = kvPairs.get(key);
		queue.remove(tupleToDelete);
	}
}
