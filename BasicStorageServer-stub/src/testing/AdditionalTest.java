package testing;


import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessageItem;
import common.messages.Marshaller;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	@Test
	public void testMarshalAndUnmarshalGetMessage() {
		KVMessage message = new KVMessageItem(StatusType.GET, "12345");
		byte[] marshaledMessage = Marshaller.marshal(message);
		KVMessage unmarshaledMessage = Marshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(StatusType.GET));
		assertTrue(unmarshaledMessage.getKey().equals("12345"));
		assertNull(unmarshaledMessage.getValue());
	}
	
	@Test
	public void testMarshalAndUnmarshalGetSuccessMessage() {
		KVMessage message = new KVMessageItem(StatusType.GET_SUCCESS, "foundValue");
		byte[] marshaledMessage = Marshaller.marshal(message);
		KVMessage unmarshaledMessage = Marshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(StatusType.GET_SUCCESS));
		assertNull(unmarshaledMessage.getKey());
		assertTrue(unmarshaledMessage.getValue().equals("foundValue"));
	}
	
	@Test
	public void testMarshalAndUnmarshalPutMessage() {
		KVMessage message = new KVMessageItem(StatusType.PUT, "123", "value");
		byte[] marshaledMessage = Marshaller.marshal(message);
		KVMessage unmarshaledMessage = Marshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(StatusType.PUT));
		assertTrue(unmarshaledMessage.getKey().equals("123"));
		assertTrue(unmarshaledMessage.getValue().equals("value"));
	}
	
	@Test
	public void testMarshalAndUnmarshalPutSuccessMessage() {
		KVMessage message = new KVMessageItem(StatusType.PUT_SUCCESS);
		byte[] marshaledMessage = Marshaller.marshal(message);
		KVMessage unmarshaledMessage = Marshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(StatusType.PUT_SUCCESS));
		assertNull(unmarshaledMessage.getKey());
		assertNull(unmarshaledMessage.getValue());
	}
}
