package common.messages;

import java.nio.charset.Charset;

import common.logic.KVServerItem;

public abstract class Marshaller<T> {
	
	protected static final char UNIT_SEPARATOR = (char) 30; //Should be generated in hash function with very low probability
	protected static final char CARRIAGE = (char) 13;
	protected final String SERVER_ATTRIBUTE_SEPARATOR = "4k6S"; //random string to reduce possibility of generating same byte sequence in start and end index
	protected static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	public abstract T unmarshal(byte[] message);
	
	public abstract byte[] marshal(T message);
	
	/**
	 * Method to convert received byte array into string array. Splits message based on UNIT_SEPARATOR
	 * character. Removes carriage from the end of the message;
	 */
	protected String[] getMessageTokens(byte[] message){
		String receivedMessage = new String(message, CHARSET);
		receivedMessage = receivedMessage.substring(0, receivedMessage.length() - 1);
		return receivedMessage.split(String.valueOf(UNIT_SEPARATOR));
	}
	
	protected String convertKVServerItemToString(KVServerItem server) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(server.getName());
		stringBuilder.append(SERVER_ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getIp());
		stringBuilder.append(SERVER_ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getPort());
		stringBuilder.append(SERVER_ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getStartIndex(), CHARSET));
		stringBuilder.append(SERVER_ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getEndIndex(), CHARSET));
		return stringBuilder.toString();
	}
	
	protected KVServerItem convertStringToMetaDataTableServer(String message)  {
		String[] serverTokens = message.split(SERVER_ATTRIBUTE_SEPARATOR);
		KVServerItem kvServer = new KVServerItem(serverTokens[0], serverTokens[1], serverTokens[2]);
		kvServer.setStartIndex(serverTokens[3].getBytes(CHARSET));
		kvServer.setEndIndex(serverTokens[4].getBytes(CHARSET));
		return kvServer;
	}
}
