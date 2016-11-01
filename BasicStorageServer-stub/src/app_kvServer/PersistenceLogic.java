package app_kvServer;

import org.apache.log4j.Logger;

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
	private Logger logger = Logger.getRootLogger();
	
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
	
	public synchronized KVMessage put(String key, String value) {
		if (cache.contains(key)) { 
			if (value.equals("null")) {
				logger.debug(Thread.currentThread() + "Delete value from cache with key: "+key);
				cache.deleteValueFor(key);
				return new KVMessageItem(StatusType.DELETE_SUCCESS);
			}
			logger.debug(Thread.currentThread() + "Update key "+key+" with value "+value);
			cache.updateElement(key, value); 
			return new KVMessageItem(StatusType.PUT_UPDATE, value);
		}
		else {
			if (value == null) {
				logger.debug(Thread.currentThread() + " Value is null, can not be updated.");
				return new KVMessageItem(StatusType.DELETE_ERROR);
			}
			putElementToCache(key, value);
			return new KVMessageItem(StatusType.PUT_SUCCESS);
		}
	}
	
	private void putElementToCache(String key, String value) {
		if (cache.size() == cacheSize) {
			logger.debug(Thread.currentThread() + "Cache size equals maximum cache size.");
			KVTuple tuple = cache.deleteElement();
			logger.debug(Thread.currentThread() + "Delete element from cache with key: "+tuple.getKey()+", value: "+tuple.getValue());
			storageCommunicator.write(tuple.getKey(), tuple.getValue());
			logger.debug(Thread.currentThread() + "Add line to storage with key: "+tuple.getKey()+", value: "+tuple.getValue());
		}
		logger.debug(Thread.currentThread() + "Add element to cache with with key: " + key + ", value: " + value);
		cache.addElement(key, value);
	}
	
	public synchronized KVMessage get(String key) {
		if (cache.contains(key)) {
			logger.debug(Thread.currentThread() + "Cache contains key "+key);
			return new KVMessageItem(StatusType.GET_SUCCESS, cache.getValueFor(key));
		} else {
			KVTuple tuple = lookUpElementOnDisk(key);
			if (tuple.getValue() != null) {
				logger.debug(Thread.currentThread() + "Element on disk was found."
						+ " Value is not equal to null. Should write to cache. "
						+ "Key: "+key+", value: "+tuple.getValue());
				putElementToCache(tuple.getKey(), tuple.getValue());
				return new KVMessageItem(StatusType.GET_SUCCESS, tuple.getValue());
			} else {
				return new KVMessageItem(StatusType.GET_ERROR);
			}
		} 
	}
	
	private KVTuple lookUpElementOnDisk(String key) {
		logger.debug(Thread.currentThread() + "Look for key " + key + " on disk");
		String value = storageCommunicator.readValueFor(key);
		return new KVTuple(key, value);
	}
	
}