package app_kvServer.subscription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In this class all subscription are controlled. The main element is a map, that maps each key to 
 * the list of ClientSubscription elements, which in turn contain information about the port and ip of 
 * the subscriber.
 *
 */
public class SubscriptionController {
	
	/**
	 * Map that connects maps a key to the list of all subscriptions to this key
	 */
	private Map<String, List<ClientSubscription>> keySubscription; 
	
	public SubscriptionController() {
		keySubscription = new HashMap<>();
	}
	
	public void addSubscription(String key, ClientSubscription client) {
		createNewListForKey(key);
		if (!keySubscription.get(key).contains(client)) keySubscription.get(key).add(client);
	}

	private void createNewListForKey(String key) {
		if (!keySubscription.containsKey(key)) {
			List<ClientSubscription> clientsForKey = new ArrayList<>();
			keySubscription.put(key, clientsForKey);
		}
	}
	
	public void removeSubscription(String key, ClientSubscription client) {
		if (key.contains(key) && keySubscription.get(key).contains(client)) keySubscription.get(key).remove(client);
	}
	
	public List<ClientSubscription> getSubscriptionListForKey(String key) {
		if (keySubscription.containsKey(key)) return keySubscription.get(key);
		else return null;
	}
	
	public boolean isKeySubscribedByClient(String key) {
		return keySubscription.containsKey(key);
	}
	
	public void deleteKey(String key) {
		if (keySubscription.containsKey(key)) keySubscription.remove(key);
	}
}
