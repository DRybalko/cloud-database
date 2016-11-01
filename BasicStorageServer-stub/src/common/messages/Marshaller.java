package common.messages;

import java.nio.charset.Charset;

import common.messages.KVMessage.StatusType;

/**
 * The class Marshaller provides the functions needed to convert from
 * byte Array to KVMessage and vice versa. 
 */


public class Marshaller {

	private static final char UNIT_SEPARATOR = (char) 31;	
	private static final char CARRIAGE = (char) 13;
	private static final String CHARSET = "US-ASCII";

	public static byte[] marshal(KVMessage message) {
		StringBuilder stringBuilder = new StringBuilder();	
		String type = new String(message.getStatus().name());

		stringBuilder.append(type);
		stringBuilder.append(UNIT_SEPARATOR);
		if(message.getKey() != null){
			stringBuilder.append(message.getKey());
			stringBuilder.append(UNIT_SEPARATOR);
		}
		if(message.getValue() != null){
			stringBuilder.append(message.getValue());
		}
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes();
	};

	public static KVMessage unmarshal(byte[] message) {		
		String[] messageTokens = getMessageTokens(message);
		StatusType type = StatusType.valueOf(messageTokens[0]);
		return createMessage(messageTokens, type);
	}
	
	private static KVMessage createMessage(String[] messageTokens, StatusType type){
		KVMessageItem message = new KVMessageItem(type);
		if (type.equals(StatusType.GET_SUCCESS)) {
			message.setValue(messageTokens[1]);
		} else if (type.equals(StatusType.GET)) {
			message.setKey(messageTokens[1]);
		} else if(type.equals(StatusType.PUT)) {
			message.setKey(messageTokens[1]);
			message.setValue(messageTokens[2]);
		} else if (type.equals(StatusType.PUT_UPDATE)) {
			message.setValue(messageTokens[1]);
		}
		return message;
	}
	
	private static String[] getMessageTokens(byte[] message){
		String receivedMessage = new String(message, Charset.forName(CHARSET));
		receivedMessage = receivedMessage.substring(0, receivedMessage.length() - 1);
		return receivedMessage.split(String.valueOf(UNIT_SEPARATOR));
	}

}
