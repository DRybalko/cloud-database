package app_kvServer.cache;

import java.util.HashMap;
import java.util.PriorityQueue;

import common.logic.Value;

import app_kvServer.KVTuple;

/**
 * That is the implementation of the LFU cache strategy.
 * The class provides methods for insertion/deletion
 * of a <key>,<value> tuple, finding/deletion a value of a key,
 * and other, that work on the least frequently used principle.
 * 
 * @see CacheStrategy
 */

public class LfuCacheStrategy implements CacheStrategy {

	private PriorityQueue<LfuQueueNode> queue;
	private HashMap<String, LfuQueueNode> kvPairs;
	
	public LfuCacheStrategy() {
		kvPairs = new HashMap<>();
		queue = new PriorityQueue<LfuQueueNode>(new LfuQueueNodeComparator());
	}

	public void addElement(String key, Value value) {
		LfuQueueNode newNode = new LfuQueueNode(new KVTuple(key, value));
		kvPairs.put(key, newNode);
		queue.add(newNode);
	}
	
	public Value getValueFor(String key) {
		LfuQueueNode node = kvPairs.get(key);
		queue.remove(node);
		node.setFreqeuncy(node.getFreqeuncy() + 1);
		queue.add(node);
		return node.getTuple().getValue();
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, Value value) {
		LfuQueueNode node = kvPairs.get(key);
		queue.remove(node);
		node.setFreqeuncy(node.getFreqeuncy() + 1);
		node.getTuple().setValue(value);
		queue.add(node);
	}

	public KVTuple deleteElement() {
		LfuQueueNode nodeToDelete = queue.poll();
		kvPairs.remove(nodeToDelete.getTuple().getKey());
		return nodeToDelete.getTuple();
	}
	
	public int size() {
		return kvPairs.size();
	}
	
	public void deleteValueFor(String key) {
		LfuQueueNode nodeToDelete = kvPairs.remove(key);
		queue.remove(nodeToDelete);
	}
}
