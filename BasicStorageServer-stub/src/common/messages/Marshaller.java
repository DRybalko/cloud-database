package common.messages;

import java.nio.charset.Charset;
import java.util.Arrays;

import common.messages.Message.MessageType;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessageMarshaller;
import common.messages.ecsToServerMessage.ECSMessageItem;
import common.messages.ecsToServerMessage.ECSMessageMarshaller;
import common.messages.serverToServerMessage.ServerToServerMessageItem;
import common.messages.serverToServerMessage.ServerToServerMessageMarshaller;

public class Marshaller {
	
	private static final String UNIT_SEPARATOR = "9c3V%_"; //Should be generated in hash function with very low probability
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	public Message unmarshal(byte[] message) {
		String[] messageTokens = getMessageTokens(message);
		String[] messageDataLoad = Arrays.copyOfRange(messageTokens, 1, messageTokens.length);
		if (messageTokens[0].equals(MessageType.CLIENT_TO_SERVER.toString())) {
			return KVMessageMarshaller.unmarshal(messageDataLoad);
		} else if (messageTokens[0].equals(MessageType.ECS_TO_SERVER.toString())) {
			return ECSMessageMarshaller.unmarshal(messageDataLoad);
		} else if (messageTokens[0].equals(MessageType.SERVER_TO_SERVER.toString())) {
			return ServerToServerMessageMarshaller.unmarshal(messageDataLoad);
		}
		return null;
	}
	
	public byte[] marshal(Message message) {
		if (message instanceof KVMessageItem) {
			return KVMessageMarshaller.marshal((KVMessageItem) message);
		} else if (message instanceof ECSMessageItem) {
			return ECSMessageMarshaller.marshal((ECSMessageItem) message);
		} else if (message instanceof ServerToServerMessageItem) {
			return ServerToServerMessageMarshaller.marshal((ServerToServerMessageItem) message);
		}
		return null;
	}
	
	private static String[] getMessageTokens(byte[] message){
		String receivedMessage = new String(message, CHARSET);
		receivedMessage = receivedMessage.substring(0, receivedMessage.length() - 1);
		return receivedMessage.split(UNIT_SEPARATOR);
	}
	
}
