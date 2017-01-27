package common.messages.clientToServerMessage;

import common.logic.KVServerItem;
import common.logic.Value;
import common.messages.Message;

/**
 * The class KVMessageItem provides three different constructors 
 * for the type KVMessageImpl. The different constructors are required 
 * for the different status types.
 *
 * @see KVMessage
 */
public class KVMessageItem extends Message implements KVMessage{

	private String key;
	private Value value;
	private KvStatusType status;
	private KVServerItem server;
	private int version;
	private String port;
	
	public KVMessageItem(KvStatusType type){
		this.status = type;
	}
	
	public KVMessageItem(KvStatusType type, String key, Value value){
		this.key = key;
		this.value = value;
		this.status = type;
	}
	
	public KVMessageItem(KvStatusType status, Value value) {
		this.status = status;
		this.value = value;
	}
	
	public KVMessageItem(KvStatusType type, String key, int version) {
		this.status = type;
		this.key = key;
		this.version = version;
	}

	public KVMessageItem(KvStatusType type, int version) {
		this.status = type;
		this.version = version;
	}
	
	@Override
	public String toString() {
		String message = "";
		if (this.status.equals(KvStatusType.GET_ERROR)) {
			message = "Get operation failed.";
		} else if (this.status.equals(KvStatusType.GET_SUCCESS)) {
			message = "Returned value is: " + this.getValue();
		} else if (this.status.equals(KvStatusType.PUT_SUCCESS)) {
			message = "Put operation was successful.";
		} else if (this.status.equals(KvStatusType.PUT_UPDATE)) {
			message = "New value for key was set.";
		} else if (this.status.equals(KvStatusType.PUT_ERROR)) {
			message = "Put operation failed.";
		} else if (this.status.equals(KvStatusType.DELETE_SUCCESS)) {
			message = "Value was deleted successfuly.";
		} else if (this.status.equals(KvStatusType.DELETE_ERROR)) {
			message = "Delete operation failed";
		} else if (this.status.equals(KvStatusType.SERVER_STOPPED)) {
			message = "Server is currently stopped. Try to send message later";
		} else if (this.status.equals(KvStatusType.SERVER_WRITE_LOCK)) {
			message = "Server can not currently process put request. Get request are still working!";
		} else if (this.status.equals(KvStatusType.VERSION)) {
			message = "For this key there are many versions. Please specify which one you would like to get";
		} else if (this.status.equals(KvStatusType.SUBSCRIPTION_SUCCESS)) {
			message = "Subscription was successful";
		} else if (this.status.equals(KvStatusType.KEY_NOT_EXISTS)) {
			message = "Subscription is not possible, because key does not exist in a system";
		} else if (this.status.equals(KvStatusType.UNSUBSCRIBE_SUCCESS)) {
			message = "Subscription stopped";
		} else if (this.status.equals(KvStatusType.NO_PERMISSION)) {
			message = "You have no permission to access this data";
		} else {
			message = "Undefined message status";
		}
		return message;	
	}

	public String getKey() {
		return key;	
	}

	public Value getValue() {
		return this.value;
	}

	public void setValue(Value value){
		this.value = value;
	}

	public void setKey(String key){
		this.key = key;
	}

	public KVServerItem getServer() {
		return server;
	}

	public void setServer(KVServerItem server) {
		this.server = server;
	}

	public KvStatusType getStatus() {
		return status;
	}

	public void setStatus(KvStatusType status) {
		this.status = status;
	}

	public void setVersion(int version) {
		this.version = version;
	}
	
	public int getVersion() {
		return version;
	}
	
	public void setPort(String port) {
		this.port = port;
	}
	
	public String getPort() {
		return port;
	}
}
