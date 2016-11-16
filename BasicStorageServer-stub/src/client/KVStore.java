package client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import common.logic.Communicator;
import common.logic.HashGenerator;
import common.logic.KVServerItem;
import common.logic.MetaDataTableController;
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
	private final KVServerItem initialKVServerItem = new KVServerItem("node1", "127.0.0.1", "50000");
	
	private Communicator<KVMessage> communicator;
	private Logger logger;
	private MetaDataTableController metaDataTableController;

	public KVStore() {
		logger = Logger.getRootLogger();
		List<KVServerItem> availableServers = new LinkedList<>();
		availableServers.add(initialKVServerItem);
		metaDataTableController = new MetaDataTableController(availableServers);
		metaDataTableController.initializeTable(1);
	}
	
	public void connect() throws IOException {
		communicator = new Communicator<KVMessage>(new KVMessageMarshaller());
	}

	public void disconnect() {
		communicator.disconnect();
	}

	public KVMessage put(String key, String value) throws Exception {
		if (communicator == null) throw new Exception("No connection!");
		KVMessage kvMessage = new KVMessageItem(KvStatusType.PUT, key, value);
		return sendMessage(kvMessage);
	}

	public KVMessage get(String key) {
		KVMessage kvMessage = new KVMessageItem(KvStatusType.GET, key);
		return sendMessage(kvMessage);
	}

	private KVMessage sendMessage(KVMessage kvMessage) {
		byte[] hashedTuple = HashGenerator.generateHashFor(kvMessage.getKey());
		KVServerItem responsibleServer = metaDataTableController.findResponsibleServer(hashedTuple);
		KVMessage reply = communicator.sendMessage(responsibleServer, kvMessage);
		if (isServerNotResponsible(reply)) {
			metaDataTableController.addServerToMetaData(reply.getServer());
			communicator.sendMessage(reply.getServer(), kvMessage);
		}
		return reply;
	}
	
	private boolean isServerNotResponsible(KVMessage message) {
		return message.getStatus().equals(KvStatusType.SERVER_NOT_RESPONSIBLE);
	}
}
