package common.messages.serverToServerMessage;

import java.nio.charset.Charset;

import common.messages.Message.MessageType;
import common.messages.serverToServerMessage.ServerToServerMessage.ServerToServerStatusType;

public class ServerToServerMessageMarshaller {

	private static final String UNIT_SEPARATOR = "9c3V%_"; //Should be generated in hash function with very low probability
	private static final char CARRIAGE = (char) 13;
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	public static byte[] marshal(ServerToServerMessageItem message) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(MessageType.SERVER_TO_SERVER);
		stringBuilder.append(UNIT_SEPARATOR);
		String type = message.getStatus().name();
		stringBuilder.append(type);
		stringBuilder.append(UNIT_SEPARATOR);
		if(message.getKey() != null){
			stringBuilder.append(message.getKey());
			stringBuilder.append(UNIT_SEPARATOR);
		}
		if (message.getIp() != null) {
			stringBuilder.append(message.getIp());
			stringBuilder.append(UNIT_SEPARATOR);
		}
		if (message.getPort() != null) {
			stringBuilder.append(message.getPort());
		}
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes(CHARSET);
	}
	
	public static ServerToServerMessageItem unmarshal(String[] messageTokens) {
		ServerToServerStatusType type = ServerToServerStatusType.valueOf(messageTokens[0]);
		ServerToServerMessageItem message = new ServerToServerMessageItem(ServerToServerStatusType.valueOf(messageTokens[0]));
		message.setMessageType(MessageType.SERVER_TO_SERVER);
		if (type.equals(ServerToServerStatusType.SUBSCRIBE) || type.equals(ServerToServerStatusType.UNSUBSCRIBE)) {
			message.setKey(messageTokens[1]);
			message.setIp(messageTokens[2]);
			message.setPort(messageTokens[3]);
		} else if (type.equals(ServerToServerStatusType.DELETE_SUBSCRIPTION_KEY)) {
			message.setKey(messageTokens[1]);
		} 
		return message;
	}
}
