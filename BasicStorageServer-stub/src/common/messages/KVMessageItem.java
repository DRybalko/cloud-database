package common.messages;

/**
 * The class KVMessageItem provides three different constructors 
 * for the type KVMessageImpl. The different constructors are required 
 * for the different status types.
 *
 * @see KVMessage
 */

public class KVMessageItem implements KVMessage{

	private String key;
	private String value;
	private KvStatusType status;

	public KVMessageItem(KvStatusType type, String key, String value){
		this.key = key;
		this.value = value;
		this.status = type;
	}

	public KVMessageItem(KvStatusType type, String keyOrValue){
		this.status = type;
		if (type.equals(KVMessage.KvStatusType.GET)) {
			this.key = keyOrValue;
		} else if (type.equals(KVMessage.KvStatusType.GET_SUCCESS) ||
				type.equals(KVMessage.KvStatusType.PUT_UPDATE)) {
			this.value = keyOrValue;
		}				
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
		} else {
			message = "Undefined status";
		}
		return message;	
	}
	
	public KVMessageItem(KvStatusType type){
		this.status = type;
	}

	public String getKey() {
		return key;	
	}

	public String getValue() {
		return value;
	}

	public KvStatusType getStatus() {
		return status;
	}

	public void setStatus(KvStatusType type) {
		this.status = type;
	}

	public void setValue(String value){
		this.value = value;
	}

	public void setKey(String key){
		this.key = key;
	}
}
