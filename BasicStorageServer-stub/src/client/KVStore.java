package client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.logic.MetaDataTableController;
import common.logic.Value;
import common.messages.clientToServerMessage.KVMessage;
import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class is responsible for the communication with the server.
 * It provides methods for establishing connection to the KV Server,
 * disconnecting the client from the currently connected server, 
 * inserting a key-value pair into the KVServer and retrieving 
 * the value for a given key from the server.
 * 
 * @see KVCommInterface
 * @see CommunicationModule
 *
 */
public class KVStore implements KVCommInterface {

	//First server to send put or get message to
	private final KVServerItem initialKVServerItem = new KVServerItem("node1", "localhost", "50000");
	
	private Communicator communicator;
	private MetaDataTableController metaDataTableController;
	private SubscriptionServer subscriptionServer;
	private int permission;
	private String username;

	public KVStore() {
		List<KVServerItem> availableServers = new LinkedList<>();
		availableServers.add(initialKVServerItem);
		metaDataTableController = new MetaDataTableController(availableServers);
		metaDataTableController.initializeTable(1);
		subscriptionServer = new SubscriptionServer(this);
		permission = 1;
		new Thread(subscriptionServer).start();
	}
	
	public void connect() throws IOException {
		communicator = new Communicator();
	}

	public void disconnect() {
		communicator.disconnect();
	}

	public KVMessage put(String key, Value value) throws Exception {
		if (communicator == null) throw new Exception("No connection!");
		KVMessage getReply = this.get(key, 1);
		if (getReply.getStatus().equals(KvStatusType.NO_PERMISSION) || (getReply.getStatus().equals(KvStatusType.GET_SUCCESS) && ((getReply.getValue() != null
				&& !getReply.getValue().getUsername().equals(this.username))))) return new KVMessageItem(KvStatusType.NO_PERMISSION);
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
		return sendMessage(kvMessage);
	}
	
	public KVMessage getVersion(String key) {
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.GET_VERSION);
		kvMessage.setKey(key);
		KVMessage versionReply = sendMessage(kvMessage);
		if (versionReply != null && versionReply.getValue() != null && 
				!hasPermission(versionReply)) return new KVMessageItem(KvStatusType.NO_PERMISSION);
		if (versionReply.getVersion() == 1) return this.get(key, 1);
		else return versionReply;
	}
	
	public KVMessage get(String key, int version) {
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.GET, key, version);
		KVMessage response = sendMessage(kvMessage);
		if (response != null && response.getValue() != null && 
				!hasPermission(response)) return new KVMessageItem(KvStatusType.NO_PERMISSION);
		else return response;
	}

	private boolean hasPermission(KVMessage message) {
		return (message.getValue().getPermission() < this.permission) || 
				(message.getValue().getPermission() == this.permission && message.getValue().getUsername().equals(this.username));
	}
	
	private KVMessageItem sendMessage(KVMessageItem kvMessage) {
		byte[] hashedTuple = HashGenerator.generateHashFor(kvMessage.getKey());
		KVServerItem responsibleServer = metaDataTableController.findResponsibleServer(hashedTuple);
		KVMessageItem reply = (KVMessageItem) communicator.sendMessage(responsibleServer, kvMessage);
		while (reply == null && !metaDataTableController.getMetaDataTable().isEmpty()) {
			responsibleServer = metaDataTableController.removeServerFromMetaData(responsibleServer);
			reply = (KVMessageItem) communicator.sendMessage(responsibleServer, kvMessage);
		}
		if (isServerNotResponsible(reply)) {
			metaDataTableController.addServerToMetaData(reply.getServer());
			reply = (KVMessageItem) communicator.sendMessage(reply.getServer(), kvMessage);
		}
		return reply;
	}

	private boolean isServerNotResponsible(KVMessage message) {
		return message.getStatus().equals(KvStatusType.SERVER_NOT_RESPONSIBLE);
	}
	
	public KVMessage sendSubscriptionStatusMessage(String key, KvStatusType status) {
		KVMessageItem getMessage = new KVMessageItem(KvStatusType.GET, key, 1);
		KVMessage response = sendMessage(getMessage);
		if (response.getStatus().equals(KvStatusType.GET_SUCCESS) && !hasPermission(response)) 
			return new KVMessageItem(KvStatusType.NO_PERMISSION);
		KVMessageItem message = new KVMessageItem(status);
		message.setKey(key);
		message.setPort("" + subscriptionServer.getPort());
		return sendMessage(message);
	}
	
	public int getPermission() {
		return permission;
	}
	
	public void setPermission(int permission) {
		this.permission = permission;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getUsername() {
		return username;
	}
}
