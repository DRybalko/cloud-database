package common.messages;

public interface ECSMessage {

	public enum EcsStatusType {
		START, 					
		STOP,
		SHUT_DOWN,
		UPDATE_START_INDEX,
		REQUEST_ACCEPTED,
		ERROR
	}	

	/**
	 * @return a status string that is used to identify request types
	 *  associated to the message.
	 */
	public EcsStatusType getStatus();
	

	/**
	 * @return data load to corresponding status type, e.g. index for UPDATE_START_INDEX
	 */
	public byte[] getDataLoad();
}
