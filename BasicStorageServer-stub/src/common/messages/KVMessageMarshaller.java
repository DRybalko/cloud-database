package common.messages;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import common.logic.KVServerItem;
import common.logic.Value;
import common.messages.KVMessage.KvStatusType;
import common.messages.Message.MessageType;

/**
 * The class Marshaller provides the functions needed to convert from
 * byte Array to KVMessage and vice versa. 
 */
public class KVMessageMarshaller {

	private static final String UNIT_SEPARATOR = "9c3V%_"; //Should be generated in hash function with very low probability
	private static final char CARRIAGE = (char) 13;
	private static final String ATTRIBUTE_SEPARATOR = "a2Lv-`;1"; //random string to reduce possibility of generating same byte sequence in start and end index
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	public static byte[] marshal(KVMessage message) {
		StringBuilder stringBuilder = new StringBuilder();	
		stringBuilder.append(MessageType.CLIENT_TO_SERVER.toString());
		stringBuilder.append(UNIT_SEPARATOR);
		String type = message.getStatus().name();
		stringBuilder.append(type);
		stringBuilder.append(UNIT_SEPARATOR);
		if(message.getKey() != null){
			stringBuilder.append(message.getKey());
			stringBuilder.append(UNIT_SEPARATOR);
		}
		if(message.getValue() != null){
			stringBuilder.append(message.getValue());
		}
		if(message.getStatus().equals(KvStatusType.SERVER_NOT_RESPONSIBLE)) {
			stringBuilder.append(convertKVServerItemToString(message.getServer()));
		}
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes();
	};

	public static KVMessageItem unmarshal(String[] messageTokens) {		
		KvStatusType type = KvStatusType.valueOf(messageTokens[0]);
		return createMessage(messageTokens, type);
	} 

	private static KVMessageItem createMessage(String[] messageTokens, KvStatusType type){
		KVMessageItem message = new KVMessageItem(type);
		message.setMessageType(MessageType.CLIENT_TO_SERVER);
		if (type.equals(KvStatusType.GET_SUCCESS)) {
			message.setValue(new Value(messageTokens[1]));
		} else if (type.equals(KvStatusType.GET)) {
			message.setKey(messageTokens[1]);
		} else if(type.equals(KvStatusType.PUT) || type.equals(KvStatusType.PUT_REPLICATION)) {
			message.setKey(messageTokens[1]);
			message.setValue(new Value(messageTokens[2]));
		} else if (type.equals(KvStatusType.PUT_UPDATE)) {
			message.getValue().setValue(messageTokens[1]);
		} else if (type.equals(KvStatusType.SERVER_NOT_RESPONSIBLE)){
			message.setServer(convertStringToMetaDataTableServer(messageTokens[1]));
		}
		return message;
	}

	protected static String convertKVServerItemToString(KVServerItem server) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(server.getName());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getIp());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getPort());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getStartIndex(), CHARSET));
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getEndIndex(), CHARSET));
		return stringBuilder.toString();
	}

	protected static KVServerItem convertStringToMetaDataTableServer(String message)  {
		String[] serverTokens = message.split(ATTRIBUTE_SEPARATOR);
		KVServerItem kvServer = new KVServerItem(serverTokens[0], serverTokens[1], serverTokens[2]);
		kvServer.setStartIndex(serverTokens[3].getBytes(CHARSET));
		kvServer.setEndIndex(serverTokens[4].getBytes(CHARSET));
		return kvServer;
	}
}
