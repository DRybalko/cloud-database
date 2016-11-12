package common.messages;

import java.nio.charset.Charset;

public abstract class Marshaller<T> {
	
	protected static final char UNIT_SEPARATOR = (char) 31;	
	protected static final char CARRIAGE = (char) 13;
	protected static final String CHARSET = "US-ASCII";
	
	public abstract T unmarshal(byte[] message);
	
	public abstract byte[] marshal(T message);
	
	protected String[] getMessageTokens(byte[] message){
		String receivedMessage = new String(message, Charset.forName(CHARSET));
		receivedMessage = receivedMessage.substring(0, receivedMessage.length() - 1);
		return receivedMessage.split(String.valueOf(UNIT_SEPARATOR));
	}
	
}
