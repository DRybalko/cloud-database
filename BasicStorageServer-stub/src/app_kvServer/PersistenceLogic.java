package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageItem;
import app_kvServer.cache.CacheStrategy;
import app_kvServer.cache.FifoCacheStrategy;
import app_kvServer.cache.LfuCacheStrategy;
import app_kvServer.cache.LruCacheStrategy;

public class PersistenceLogic {

	private CacheStrategy cache;
	private int cacheSize;
	private StorageCommunicator storageCommunicator;
	
	public PersistenceLogic(int cacheSize, String cacheStrategy) {
		this.cacheSize = cacheSize;
		this.cache = defineCacheStrategy(cacheStrategy);
		this.storageCommunicator  = new StorageCommunicator();
	}
	
	private CacheStrategy defineCacheStrategy(String cacheStrategy) {
		if (cacheStrategy.toLowerCase().equals("fifo")) {
			return new FifoCacheStrategy();
		} else if (cacheStrategy.toLowerCase().equals("lfu")) {
			return new LfuCacheStrategy();
		} else if (cacheStrategy.toLowerCase().equals("lru")) {
			return new LruCacheStrategy();
		} else return null;
	}
	
	public KVMessage put(String key, String value) {
		if (cache.contains(key)) { 
			if (value.equals("null")) {
				cache.deleteValueFor(key);
				return new KVMessageItem(StatusType.DELETE_SUCCESS);
			}
			cache.updateElement(key, value); 
			return new KVMessageItem(StatusType.PUT_UPDATE, value);
		}
		else {
			if (value == null) return new KVMessageItem(StatusType.DELETE_ERROR);
			putElementToCache(key, value);
			return new KVMessageItem(StatusType.PUT_SUCCESS);
		}
	}
	
	private void putElementToCache(String key, String value) {
		if (cache.size() == cacheSize) {
			KVTuple tuple = cache.deleteElement();
			storageCommunicator.write(tuple.getKey(), tuple.getValue());
		}
		cache.addElement(key, value);
	}
	
	public KVMessage get(String key) {
		if (cache.contains(key)) {
			return new KVMessageItem(StatusType.GET_SUCCESS, cache.getValueFor(key));
		} else {
			KVTuple tuple = lookUpElementOnDisk(key);
			if (tuple.getValue() != null) {
				putElementToCache(tuple.getKey(), tuple.getValue());
				return new KVMessageItem(StatusType.GET_SUCCESS, tuple.getValue());
			} else {
				return new KVMessageItem(StatusType.GET_ERROR);
			}
		} 
	}
	
	private KVTuple lookUpElementOnDisk(String key) {
		String value = storageCommunicator.readValueFor(key);
		return new KVTuple(key, value);
	}
	
}