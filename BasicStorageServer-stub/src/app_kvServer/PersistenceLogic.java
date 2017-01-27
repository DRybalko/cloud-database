package app_kvServer;

import org.apache.log4j.Logger;

import common.logic.Value;
import common.logic.ValueMarshaler;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;
import app_kvServer.cache.CacheStrategy;
import app_kvServer.cache.FifoCacheStrategy;
import app_kvServer.cache.LfuCacheStrategy;
import app_kvServer.cache.LruCacheStrategy;

/**
 * This class is used to identify, whether key must be saved, updated or deleted. Depending on cache
 * strategy and cache size defined, when server was initialized it then persists key value pair in cache 
 * or on disk.
 */
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
	
	public synchronized KVMessage put(String key, Value value) {
		if (cache.contains(key)) { 
			if (value.getValue().trim().equals("null")) {
				return deleteKvPairFromCache(key);
			}
			return new KVMessageItem(KvStatusType.PUT_SUCCESS);
		}
		else {
			return checkKvPairInStorage(key, ValueMarshaler.marshal(value));
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
			KVMessageItem message = new KVMessageItem(KvStatusType.GET_SUCCESS);
			message.setValue(ValueMarshaler.unmarshal(cache.getValueFor(key)));
			return message;
		} else {
			KVTuple tuple = lookUpElementOnDisk(key);
			if (tuple.getValue() != null) {
				logger.debug(Thread.currentThread() + "Element on disk was found."
						+ " Value is not equal to null. Should write to cache. "
						+ "Key: "+key+", value: "+tuple.getValue());
				putElementToCache(tuple.getKey(), tuple.getValue());
				KVMessageItem message = new KVMessageItem(KvStatusType.GET_SUCCESS);
				message.setValue(ValueMarshaler.unmarshal(tuple.getValue()));
				return message;
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