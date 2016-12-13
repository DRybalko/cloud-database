package common.messages;

public interface PingMessage {

	public enum PingStatusType {
		GET_STATUS,
		IN_PROGRESS
	}
	
	public PingStatusType getStatus();
}
