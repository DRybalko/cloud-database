package app_kvServer.cache;

import common.logic.Value;

import app_kvServer.KVTuple;

/**
 * Contains all the methods that should be implemented in
 * the different cache strategies.
 */

public interface CacheStrategy {
	
	public boolean contains(String key);
	
	public void addElement(String key, Value value);
	
	public Value getValueFor(String key);

	public void updateElement(String key, Value value);
	
	public KVTuple deleteElement();
	
	public int size();
	
	public void deleteValueFor(String key);
	
}
