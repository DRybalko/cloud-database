package app_kvServer.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import app_kvServer.KVTuple;

public class LfuCacheStrategy implements CacheStrategy {

	private ArrayList<LinkedList<KVTuple>> frequencies; 
	private HashMap<String, LfuQueueNode> kvPairs;
	
	public LfuCacheStrategy() {
		kvPairs = new HashMap<>();
		frequencies = new ArrayList<>();
	}

	public void addElement(String key, String value) {
		KVTuple tuple = new KVTuple(key, value);
		kvPairs.put(key, new LfuQueueNode(tuple));
		addNewElementToFrequencies(new KVTuple(key, value));
	}
	
	private void addNewElementToFrequencies(KVTuple tuple) {
		if (frequencies.get(0) == null) {
			frequencies.set(0, new LinkedList<KVTuple>());
		} else {
			frequencies.get(0).add(tuple);
		}
	}
	
	public String getValueFor(String key) {
		LfuQueueNode node = kvPairs.get(key);
		LinkedList<KVTuple> frequencyList = frequencies.get(node.getFreqeuncy());
		for (KVTuple tuple: frequencyList) {
			if (tuple.getKey().equals(key)) {
				if (frequencies.get(node.getFreqeuncy() + 1) == null) {
					frequencies.add(node.getFreqeuncy() + 1, new LinkedList<KVTuple>());
				}
				frequencies.get(node.getFreqeuncy() + 1).add(tuple);
				return tuple.getValue();
			}
		}
		return node.getTuple().getValue();
	}

	public boolean contains(String key) {
		return kvPairs.containsKey(key);
	}

	public void updateElement(String key, String value) {
		LfuQueueNode node = kvPairs.get(key);
		LinkedList<KVTuple> frequencyList = frequencies.get(node.getFreqeuncy());
		for (KVTuple tuple: frequencyList) {
			if (tuple.getKey().equals(key)) {
				frequencyList.remove(tuple);
				tuple.setValue(value);
				frequencies.get(0).add(tuple);
				kvPairs.put(key, new LfuQueueNode(tuple));
			}
		}
	}

	public KVTuple deleteElement() {
		KVTuple tuple = new KVTuple("", "");
		for (LinkedList<KVTuple> listWithTuples: frequencies) {
			if (listWithTuples.size() > 0)  tuple = listWithTuples.removeLast();
		}
		LfuQueueNode node = kvPairs.remove(tuple.getKey());
		return node.getTuple();
	}
	
	public int size() {
		return kvPairs.size();
	}

}
