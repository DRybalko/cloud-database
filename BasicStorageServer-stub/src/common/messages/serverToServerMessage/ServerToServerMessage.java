package common.messages.serverToServerMessage;

public interface ServerToServerMessage {

	public enum ServerToServerStatusType {
		GET_STATUS,				/* Ping message to check, whether server is working */
		IN_PROGRESS,			/* Ping message reply. */
		SUBSCRIBE, 				/* Subscription on server, that is not responsible for key for replication */
    	UNSUBSCRIBE,			/* End subscription for key on replication server */
    	DELETE_SUBSCRIPTION_KEY,/* Delete key with all subscriptions */
    	SUBSCRIPTION_SUCCESS,
    	UNSUBSCRIBE_SUCCESS,
    	DELETE_SUCCESS,
    	ERROR
	}
	
	public ServerToServerStatusType getStatus();
	
	public String getKey();
	
	public String getPort();
	
	public String getIp();
	
}
