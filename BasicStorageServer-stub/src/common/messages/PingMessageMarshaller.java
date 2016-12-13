package common.messages;

import java.nio.charset.Charset;

import common.messages.Message.MessageType;
import common.messages.PingMessage.PingStatusType;

public class PingMessageMarshaller {

	private static final String UNIT_SEPARATOR = "9c3V%_"; //Should be generated in hash function with very low probability
	private static final char CARRIAGE = (char) 13;
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	public static byte[] marshal(PingMessageItem message) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(MessageType.SERVER_TO_SERVER);
		stringBuilder.append(UNIT_SEPARATOR);
		stringBuilder.append(message.getStatus());
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes(CHARSET);
	}
	
	public static PingMessageItem unmarshal(String[] messageTokens) {
		PingMessageItem pingMessage = new PingMessageItem(PingStatusType.valueOf(messageTokens[0]));
		pingMessage.setMessageType(MessageType.SERVER_TO_SERVER);
		return pingMessage;
	}
}
