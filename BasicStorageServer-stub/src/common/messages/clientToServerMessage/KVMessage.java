package common.messages.clientToServerMessage;

import common.logic.KVServerItem;
import common.logic.Value;

public interface KVMessage {
	
	public enum KvStatusType {
		GET,             		/* Get - request */
		GET_VERSION,			/* Get - request for given version */
    	GET_ERROR,       		/* requested tuple (i.e. value) not found */
    	GET_SUCCESS,     		/* requested tuple (i.e. value) found */
    	VERSION,				/* Return number of versions for given key */
    	PUT,               		/* Put - request */
    	PUT_REPLICATION,		/* Put - request from another server to store replication of KV pair */
    	PUT_SUCCESS,     		/* Put - request successful, tuple inserted */
    	PUT_UPDATE,      		/* Put - request successful, i.e., value updated */
    	PUT_ERROR,       		/* Put - request not successful */
    	DELETE_SUCCESS,  		/* Delete - request successful */
    	DELETE_ERROR,     		/* Delete - request successful */
    	SERVER_STOPPED,         /* Server is stopped, no requests are processed */
    	SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
    	SERVER_NOT_RESPONSIBLE, /* Request not successful, server not responsible for key */
    	SUBSCRIBE,				/* Subscribe for key updates */
    	SUBSCRIPTION_SUCCESS,	/* Subscription for key is successful */
    	UNSUBSCRIBE,			/* End subscription for key */
    	UNSUBSCRIBE_SUCCESS,	/* Unsubscribe success */
    	KEY_NOT_EXISTS,			/* Subscription is not possible, because key was never created */
    	NO_PERMISSION,
    	ERROR			
	}
	
	public KvStatusType getStatus();
	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public Value getValue();
	
	/**
	 * 
	 * @return the version of data for given key
	 */
	public int getVersion();
	
	/**
	 * @return a server item with corresponding start and end index
	 * They are used to identify server responsible for key.
	 */
	public KVServerItem getServer();
	
	/**
	 * @return port of the client to listen for key updates
	 */
	public String getPort();
}


