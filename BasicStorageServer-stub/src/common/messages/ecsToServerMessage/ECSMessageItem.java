package common.messages.ecsToServerMessage;

import java.util.List;
import java.util.Map;

import common.logic.KVServerItem;
import common.messages.Message;

public class ECSMessageItem extends Message implements ECSMessage{

	private byte[] startIndex;
	private byte[] endIndex;
	private EcsStatusType status;
	private List<KVServerItem> metaDataTable;
	private Map<String, String> keyValuesForDataTransfer;
	private KVServerItem serverItem;
	
	public ECSMessageItem(EcsStatusType status) {
		this.status = status;
	}
	
	public ECSMessageItem(EcsStatusType status, byte[] startIndex) {
		this.status = status;
		this.startIndex = startIndex;
	}
	
	public ECSMessageItem(EcsStatusType status, Map<String, String> keyValuesForDataTransfer) {
		this.status = status;
		this.keyValuesForDataTransfer = keyValuesForDataTransfer;
	}
	
	public ECSMessageItem(EcsStatusType status, List<KVServerItem> metaDataTable) {
		this.status = status;
		this.metaDataTable = metaDataTable;
	}
	
	public ECSMessageItem(EcsStatusType status, byte[] startIndex, byte[] endIndex) {
		this.status = status;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}
	
	public ECSMessageItem(EcsStatusType status, KVServerItem serverItem) {
		this.status = status;
		this.serverItem = serverItem;
	}

	public byte[] getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(byte[] startIndex) {
		this.startIndex = startIndex;
	}

	public byte[] getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(byte[] endIndex) {
		this.endIndex = endIndex;
	}

	public List<KVServerItem> getMetaDataTable() {
		return metaDataTable;
	}

	public void setMetaDataTable(List<KVServerItem> metaDataTable) {
		this.metaDataTable = metaDataTable;
	}

	public Map<String, String> getKeyValuesForDataTransfer() {
		return keyValuesForDataTransfer;
	}

	public void setKeyValuesForDataTransfer(
			Map<String, String> keyValuesForDataTransfer) {
		this.keyValuesForDataTransfer = keyValuesForDataTransfer;
	}

	public EcsStatusType getStatus() {
		return status;
	}

	public void setStatus(EcsStatusType status) {
		this.status = status;
	}

	public KVServerItem getServerItem() {
		return serverItem;
	}

	public void setServerItem(KVServerItem serverItem) {
		this.serverItem = serverItem;
	}
	
}
