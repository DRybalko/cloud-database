package testing;

import java.util.Arrays;

import org.junit.Test;

import common.messages.ECSMessage.EcsStatusType;
import common.messages.ECSMessageItem;
import common.messages.ECSMessageMarshaller;
import common.messages.KVMessage;
import common.messages.KVMessageItem;
import common.messages.KVMessageMarshaller;
import common.messages.KVMessage.KvStatusType;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	KVMessageMarshaller kVMessageMarshaller = new KVMessageMarshaller();
	ECSMessageMarshaller ecsMessageMarshaller = new ECSMessageMarshaller();

	@Test
	public void testMarshalAndUnmarshalGetMessage() {
		KVMessage message = new KVMessageItem(KvStatusType.GET, "12345");
		byte[] marshaledMessage = kVMessageMarshaller.marshal(message);
		KVMessage unmarshaledMessage = kVMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(KvStatusType.GET));
		assertTrue(unmarshaledMessage.getKey().equals("12345"));
		assertNull(unmarshaledMessage.getValue());
	}
	
	@Test
	public void testMarshalAndUnmarshalGetSuccessMessage() {
		KVMessage message = new KVMessageItem(KvStatusType.GET_SUCCESS, "foundValue");
		byte[] marshaledMessage = kVMessageMarshaller.marshal(message);
		KVMessage unmarshaledMessage = kVMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(KvStatusType.GET_SUCCESS));
		assertNull(unmarshaledMessage.getKey());
		assertTrue(unmarshaledMessage.getValue().equals("foundValue"));
	}
	
	@Test
	public void testMarshalAndUnmarshalPutMessage() {
		KVMessage message = new KVMessageItem(KvStatusType.PUT, "123", "value");
		byte[] marshaledMessage = kVMessageMarshaller.marshal(message);
		KVMessage unmarshaledMessage = kVMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(KvStatusType.PUT));
		assertTrue(unmarshaledMessage.getKey().equals("123"));
		assertTrue(unmarshaledMessage.getValue().equals("value"));
	}
	
	@Test
	public void testMarshalAndUnmarshalPutSuccessMessage() {
		KVMessage message = new KVMessageItem(KvStatusType.PUT_SUCCESS);
		byte[] marshaledMessage = kVMessageMarshaller.marshal(message);
		KVMessage unmarshaledMessage = kVMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshaledMessage.getStatus().equals(KvStatusType.PUT_SUCCESS));
		assertNull(unmarshaledMessage.getKey());
		assertNull(unmarshaledMessage.getValue());
	}
	
	@Test
	public void testECSMessageMarshalWithDataLoad() {
		ECSMessageItem message = new ECSMessageItem(EcsStatusType.UPDATE_START_INDEX, new byte[]{12, -3, 4});
		byte[] marshaledMessage = ecsMessageMarshaller.marshal(message);
		ECSMessageItem unmarshalledMessage = ecsMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshalledMessage.getStatus().equals(EcsStatusType.UPDATE_START_INDEX));
		assertTrue(Arrays.equals(unmarshalledMessage.getDataLoad(), new byte[]{12, -3, 4}));
	}
	
	@Test
	public void testECSMessageMarshlaWithoutDataLoad() {
		ECSMessageItem message = new ECSMessageItem(EcsStatusType.START);
		byte[] marshaledMessage = ecsMessageMarshaller.marshal(message);
		ECSMessageItem unmarshalledMessage = ecsMessageMarshaller.unmarshal(marshaledMessage);
		assertTrue(unmarshalledMessage.getStatus().equals(EcsStatusType.START));
	}

}
