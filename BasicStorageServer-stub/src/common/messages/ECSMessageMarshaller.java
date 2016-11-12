package common.messages;

import common.messages.ECSMessage.EcsStatusType;

public class ECSMessageMarshaller extends Marshaller<ECSMessage> {
	
	public byte[] marshal(ECSMessage message) {
		StringBuilder stringBuilder = new StringBuilder();	
		String type = message.getStatus().toString();
		
		stringBuilder.append(type);
		stringBuilder.append(UNIT_SEPARATOR);
		byte[] messageInBytes = stringBuilder.toString().getBytes();
		return marshalDataLoad(message.getDataLoad(), messageInBytes);
	};
	
	public ECSMessageItem unmarshal(byte[] message){
		StringBuilder stringBuilder = new StringBuilder();
		int i = 0;
		while (message[i] != UNIT_SEPARATOR) {
			stringBuilder.append((char) message[i]);
			i++;
		}
		ECSMessageItem unmarshalledMessage = new ECSMessageItem(EcsStatusType.valueOf(stringBuilder.toString()));
		if (message.length > (i + 2)) {
			byte[] dataLoad = new byte[message.length - i - 2];
			System.arraycopy(message, i+1, dataLoad, 0, message.length - i - 2);
			unmarshalledMessage.setDataLoad(dataLoad);
		}
		return unmarshalledMessage;
	}	
	
	private static byte[] marshalDataLoad(byte[] dataLoad, byte[] messageInBytes) {
		byte[] marshaledMessage;
		if (dataLoad == null) {
			marshaledMessage = new byte[messageInBytes.length + 1];
		} else {
			 marshaledMessage = new byte[messageInBytes.length + dataLoad.length + 1];
			 System.arraycopy(dataLoad, 0, marshaledMessage, messageInBytes.length, dataLoad.length);
		}
		System.arraycopy(messageInBytes, 0, marshaledMessage, 0, messageInBytes.length);
		marshaledMessage[marshaledMessage.length - 1] = 13;
		return marshaledMessage;
	}
}
