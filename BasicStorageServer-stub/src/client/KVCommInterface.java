package client;

import common.logic.Value;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

public interface KVCommInterface {

	/**
	 * Establishes a connection to the KV Server.
	 * 
	 * @throws Exception
	 *             if connection could not be established.
	 */
	public void connect() throws Exception;

	/**
	 * disconnects the client from the currently connected server.
	 */
	public void disconnect();

	/**
	 * Inserts a key-value pair into the KVServer.
	 * 
	 * @param key
	 *            the key that identifies the given value.
	 * @param value
	 *            the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public KVMessage put(String key, Value value) throws Exception;

	/**
	 * Retrieves the value for a given key from the KVServer.
	 * 
	 * @param key
	 *            the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public KVMessage get(String key, int version) throws Exception;
	
	public KVMessage getVersion(String key) throws Exception;
	
	public KVMessage sendSubscriptionStatusMessage(String key, KvStatusType status) throws Exception;
	
	public void setPermission(int permission);
	
	public int getPermission();
	
	public void setUsername(String username);
	
	public String getUsername();
}
