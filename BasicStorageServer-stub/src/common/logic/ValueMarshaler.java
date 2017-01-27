package common.logic;

import java.time.LocalDateTime;

public class ValueMarshaler {

	private static final String ATTRIBUTE_SEPARATOR = ";";
	
	public static String marshal(Value value) {
		String marshalledValue = "";
		marshalledValue += value.getPermission() + ATTRIBUTE_SEPARATOR;
		marshalledValue += value.getTimestamp() + ATTRIBUTE_SEPARATOR;
		return marshalledValue + value.getValue();
	}
	
	public static Value unmarshal(String value) {
		String[] valueTokens = value.split(ATTRIBUTE_SEPARATOR);
		int permission = Integer.parseInt(valueTokens[0]);
		LocalDateTime timestamp = LocalDateTime.parse(valueTokens[1]);
		return new Value(permission, timestamp, value.substring(valueTokens[0].length() + valueTokens[1].length() + 2));
	}
	
}
