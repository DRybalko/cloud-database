package common.messages;

import java.util.List;

import common.logic.KVServerItem;

public interface ECSMessage {

	public enum EcsStatusType {
		START, 					
		STOP,
		SHUT_DOWN,
		UPDATE_START_INDEX,
		META_DATA_TABLE,
		SERVER_START_END_INDEX,
		REQUEST_ACCEPTED,
		DATA_TRANSFERED,
		ERROR
	}	

	/**
	 * @return a status string that is used to identify request types
	 *  associated to the message.
	 */
	public EcsStatusType getStatus();
	
	public byte[] getStartIndex();
	
	public byte[] getEndIndex();
	
	public List<KVServerItem> getMetaDataTable();
	
}
