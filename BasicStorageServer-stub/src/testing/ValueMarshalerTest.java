package testing;

import static org.junit.Assert.*;

import java.time.LocalDateTime;

import org.junit.Test;

import common.logic.Value;
import common.logic.ValueMarshaler;

public class ValueMarshalerTest {

	@Test
	public void shouldConvertValueToString() {
		Value value = new Value(2, LocalDateTime.of(2017, 1, 5, 17, 1), "This is a real value");
		String marshalledValue = ValueMarshaler.marshal(value);
		assertTrue(marshalledValue.equals("2;2017-01-05T17:01;This is a real value"));
	}
	
	@Test
	public void shouldUnmarshalStringToValue() {
		String value = "2;2017-01-05T17:01;This is a real value";
		Value valueObject = ValueMarshaler.unmarshal(value);
		assertTrue(valueObject.getPermission() == 2);
		assertTrue(valueObject.getTimestamp().equals(LocalDateTime.of(2017, 1, 5, 17, 1)));
		assertTrue(valueObject.getValue().equals("This is a real value"));
	}
}
