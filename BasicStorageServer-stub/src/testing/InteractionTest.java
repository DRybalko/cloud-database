package testing;

import java.time.LocalDateTime;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.logic.Value;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

public class InteractionTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore();
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void testPut() {
		String key = "foobar";
		Value value = new Value(1, LocalDateTime.now(), "bar");
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == KvStatusType.PUT_SUCCESS);
	}
	
	// Test is not relevant, because the connection problem is caught and logged.

	/*public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		Value value = new Value(1, LocalDateTime.now(), "bar");
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}*/

	//First initial value is not updated anymore, but another version for the given key is created. In a current
	//setting up to 5 versions can be persisted for one key.
	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		Value valueInitial = new Value(1, LocalDateTime.now(), "initial");
		Value valueUpdated = new Value(1, LocalDateTime.now(), "updated");
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, valueInitial);
			response = kvClient.put(key, valueUpdated);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == KvStatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		Value value = new Value(1, LocalDateTime.now(), "toDelete");
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, new Value(1, LocalDateTime.now(), "null"));
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == KvStatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		LocalDateTime currentTimeStamp = LocalDateTime.now();
		Value value = new Value(1, currentTimeStamp, "bar");
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key, 1);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().getValue().equals("bar"));
		assertTrue(response.getValue().getPermission() == 1);
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key, 1);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == KvStatusType.GET_ERROR);
	}
	
	@Test
	public void testDeleteNonExistingKvPair() {
		String key = "keyForNull";
		Value value = new Value(1, LocalDateTime.now(), "null");
		Exception ex = null;
		KVMessage response = null;
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == KvStatusType.DELETE_SUCCESS);
	}

}
