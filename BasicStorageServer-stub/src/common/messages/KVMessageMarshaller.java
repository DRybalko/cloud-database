package common.messages;

import common.messages.KVMessage.KvStatusType;

/**
 * The class Marshaller provides the functions needed to convert from
 * byte Array to KVMessage and vice versa. 
 */


public class KVMessageMarshaller extends Marshaller<KVMessage> {

	public byte[] marshal(KVMessage message) {
		StringBuilder stringBuilder = new StringBuilder();	
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
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes();
	};

	public KVMessage unmarshal(byte[] message) {		
		String[] messageTokens = getMessageTokens(message);
		KvStatusType type = KvStatusType.valueOf(messageTokens[0]);
		return createMessage(messageTokens, type);
	}
	
	private static KVMessage createMessage(String[] messageTokens, KvStatusType type){
		KVMessageItem message = new KVMessageItem(type);
		if (type.equals(KvStatusType.GET_SUCCESS)) {
			message.setValue(messageTokens[1]);
		} else if (type.equals(KvStatusType.GET)) {
			message.setKey(messageTokens[1]);
		} else if(type.equals(KvStatusType.PUT)) {
			message.setKey(messageTokens[1]);
			message.setValue(messageTokens[2]);
		} else if (type.equals(KvStatusType.PUT_UPDATE)) {
			message.setValue(messageTokens[1]);
		}
		return message;
	}

}
