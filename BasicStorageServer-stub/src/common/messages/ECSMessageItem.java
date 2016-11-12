package common.messages;

public class ECSMessageItem implements ECSMessage{

	private EcsStatusType status;
	private byte[] dataLoad;
	
	public ECSMessageItem(EcsStatusType status) {
		this.status = status;
	}
	
	public ECSMessageItem(EcsStatusType status, byte[] dataLoad) {
		this.status = status;
		this.dataLoad = dataLoad;
	}

	public EcsStatusType getStatus() {
		return status;
	}

	public void setStatus(EcsStatusType status) {
		this.status = status;
	}

	public byte[] getDataLoad() {
		return dataLoad;
	}

	public void setDataLoad(byte[] dataLoad) {
		this.dataLoad = dataLoad;
	}
	
	
}
