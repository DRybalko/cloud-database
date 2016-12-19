package app_kvServer;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.KvStatusType;
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
	
	private final String CLASS_NAME = "PersistenceLogic: ";
	
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
			if (value.trim().equals("null")) {
				return deleteKvPairFromCache(key);
			}
			return updateKvPairInCache(key, value);
		}
		else {
			return checkKvPairInStorage(key, value);
		}
	}

	private KVMessage checkKvPairInStorage(String key, String value) {
		KVMessage responseMessage;
		if (value == null) {
			logger.debug(CLASS_NAME + " Value is null, can not be updated.");
			responseMessage = new KVMessageItem(KvStatusType.DELETE_ERROR);
		} else if (value.trim().equals("null")) {
			responseMessage = deleteKvPairFromStorage(key);
		} else if (storageCommunicator.readValueFor(key) != null) {
			putElementToCache(key, value);
			responseMessage = new KVMessageItem(KvStatusType.PUT_UPDATE);
		} else {
			putElementToCache(key, value);
			responseMessage = new KVMessageItem(KvStatusType.PUT_SUCCESS);
		}
		return responseMessage;
	}

	private KVMessage deleteKvPairFromStorage(String key) {
		KVMessageItem responseMessage;
		if (storageCommunicator.deleteFromStorage(key)) {
			responseMessage = new KVMessageItem(KvStatusType.DELETE_SUCCESS);
		} else {
			responseMessage = new KVMessageItem(KvStatusType.DELETE_ERROR);
		}
		return responseMessage;
	}

	private KVMessage updateKvPairInCache(String key, String value) {
		logger.debug(CLASS_NAME + " Update key "+key+" with value "+value);
		cache.updateElement(key, value); 
		if (storageCommunicator.readValueFor(key) != null) {
			storageCommunicator.put(key, value);
		}
		return new KVMessageItem(KvStatusType.PUT_UPDATE, value);
	}

	private KVMessage deleteKvPairFromCache(String key) {
		logger.debug(CLASS_NAME + " Delete value from cache with key: "+key);
		cache.deleteValueFor(key);
		storageCommunicator.deleteFromStorage(key);
		return new KVMessageItem(KvStatusType.DELETE_SUCCESS);
	}
	
	private void putElementToCache(String key, String value) {
		if (cache.size() == cacheSize) {
			logger.debug(Thread.currentThread() + "Cache size equals maximum cache size.");
			KVTuple tuple = cache.deleteElement();
			logger.debug(Thread.currentThread() + "Delete element from cache with key: "+tuple.getKey()+", value: "+tuple.getValue());
			storageCommunicator.put(tuple.getKey(), tuple.getValue());
			logger.debug(Thread.currentThread() + "Add line to storage with key: "+tuple.getKey()+", value: "+tuple.getValue());
		}
		logger.debug(Thread.currentThread() + "Add element to cache with with key: " + key + ", value: " + value);
		cache.addElement(key, value);
	}
	
	public synchronized KVMessage get(String key) {
		if (cache.contains(key)) {
			logger.debug(Thread.currentThread() + "Cache contains key "+key);
			return new KVMessageItem(KvStatusType.GET_SUCCESS, cache.getValueFor(key));
		} else {
			KVTuple tuple = lookUpElementOnDisk(key);
			if (tuple.getValue() != null) {
				logger.debug(Thread.currentThread() + "Element on disk was found."
						+ " Value is not equal to null. Should write to cache. "
						+ "Key: "+key+", value: "+tuple.getValue());
				putElementToCache(tuple.getKey(), tuple.getValue());
				return new KVMessageItem(KvStatusType.GET_SUCCESS, tuple.getValue());
			} else {
				return new KVMessageItem(KvStatusType.GET_ERROR);
			}
		} 
	}
	
	private KVTuple lookUpElementOnDisk(String key) {
		logger.debug(Thread.currentThread() + "Look for key " + key + " on disk");
		String value = storageCommunicator.readValueFor(key);
		return new KVTuple(key, value);
	}
	
}