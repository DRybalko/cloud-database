package common.messages;

public class PingMessageItem extends Message implements PingMessage {
	
	private PingStatusType status;
	
	public PingMessageItem(PingStatusType status) {
		this.status = status;
	}
	
	public PingStatusType getStatus() {
		return this.status;
	}
}
