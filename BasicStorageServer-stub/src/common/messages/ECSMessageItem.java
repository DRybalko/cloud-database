package common.messages;

import java.util.List;

import common.logic.KVServerItem;

public class ECSMessageItem implements ECSMessage{

	private EcsStatusType status;
	private byte[] startIndex;
	private byte[] endIndex;
	private List<KVServerItem> metaDataTable;
	
	public ECSMessageItem(EcsStatusType status) {
		this.status = status;
	}
	
	public ECSMessageItem(EcsStatusType status, byte[] startIndex) {
		this.status = status;
		this.startIndex = startIndex;
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

	public EcsStatusType getStatus() {
		return status;
	}

	public void setStatus(EcsStatusType status) {
		this.status = status;
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

	
	
}
