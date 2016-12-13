package common.messages;

import java.util.List;
import java.util.Map;

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
		ECS_METADATA,
		FAULTY_SERVER,
		REALLOCATE_DATA,
		REMOVE_FAULTY_SERVER,
		SERVER_STOPPED
	}
	
	public EcsStatusType getStatus();
	
	public byte[] getStartIndex();
	
	public byte[] getEndIndex();
	
	public List<KVServerItem> getMetaDataTable();
	
	public Map<String, String> getKeyValuesForDataTransfer();
	
	public KVServerItem getServerItem();
}
