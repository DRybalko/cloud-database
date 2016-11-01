package app_kvServer.cache;

import java.util.HashMap;
import java.util.LinkedList;

import app_kvServer.KVTuple;

/**
 * That is the implementation of the FIFO cache strategy.
 * The class provides methods for insertion/deletion
 * of a <key>,<value> tuple, finding/deletion a value of a key,
 * and other, that work on the first in first out principle.
 *
 */

public class FifoCacheStrategy implements CacheStrategy {

	private LinkedList<KVTuple> queue;
	private HashMap<String, KVTuple> kvPairs;
	
	public FifoCacheStrategy() {
		queue = new LinkedList<>();
		kvPairs = new HashMap<>();		
	}

	public void addElement(String key, String value) {
		KVTuple tupleToAdd = new KVTuple(key, value);
		kvPairs.put(key, tupleToAdd);
		queue.add(tupleToAdd);
	}
	
	public KVTuple deleteElement() {
		KVTuple tuple = queue.removeFirst();
		kvPairs.remove(tuple.getKey());
		return tuple;
	}

	public String getValueFor(String key) {
		return kvPairs.get(key).getValue();
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, String value) {
		KVTuple tupleToUpdate = kvPairs.get(key);
		tupleToUpdate.setValue(value);
	}

	public int size() {
		return kvPairs.size();
	}
	
	public void deleteValueFor(String key) {
		KVTuple tupleToDelete = kvPairs.remove(key);
		queue.remove(tupleToDelete);
	}
	
}