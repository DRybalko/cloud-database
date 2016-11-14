package common.messages;

import java.nio.charset.Charset;

public abstract class Marshaller<T> {
	
	protected static final char UNIT_SEPARATOR = (char) 31;	
	protected static final char CARRIAGE = (char) 13;
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
	
}
