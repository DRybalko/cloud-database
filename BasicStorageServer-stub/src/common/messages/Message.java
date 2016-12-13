package common.messages;

public abstract class Message {

	public enum MessageType {
		ECS_TO_SERVER,
		CLIENT_TO_SERVER,
		SERVER_TO_SERVER
	}
	
	protected MessageType messageType;

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}
	
}
