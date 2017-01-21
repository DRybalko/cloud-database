package client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.logic.MetaDataTableController;
import common.logic.Value;
import common.messages.*;
import common.messages.KVMessage.KvStatusType;

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

	public KVStore() {
		List<KVServerItem> availableServers = new LinkedList<>();
		availableServers.add(initialKVServerItem);
		metaDataTableController = new MetaDataTableController(availableServers);
		metaDataTableController.initializeTable(1);
	}
	
	public void connect() throws IOException {
		communicator = new Communicator();
	}

	public void disconnect() {
		communicator.disconnect();
	}

	public KVMessage put(String key, Value value) throws Exception {
		if (communicator == null) throw new Exception("No connection!");
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
		return sendMessage(kvMessage);
	}

	public KVMessage get(String key) {
		KVMessageItem kvMessage = new KVMessageItem(KvStatusType.GET, key);
		return sendMessage(kvMessage);
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
}
