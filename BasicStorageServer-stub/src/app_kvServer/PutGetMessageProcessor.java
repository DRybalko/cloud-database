package app_kvServer;

import common.messages.clientToServerMessage.KVMessageItem;
import common.messages.clientToServerMessage.KVMessage.KvStatusType;

/**
 * This class is used to convert normal key to key with version, which is than used for persistence in cache/on disk.
 * This conversion is needed by put as well as by get message. 
 *
 */
public class PutGetMessageProcessor {

	private KVServer server;
	
	public PutGetMessageProcessor(KVServer server) {
		this.server = server;
	}

	public KVMessageItem putKeyValue(KVMessageItem message) {
		if (!message.getStatus().equals(KvStatusType.PUT_REPLICATION)) sendReplication(message);
		if (message.getValue().getValue().trim().toLowerCase().equals("null")) {
			server.getVersionController().deleteKey(message.getKey());
			return new KVMessageItem(KvStatusType.DELETE_SUCCESS);
		}
		String keyForVersion = server.getVersionController().addKey(message.getKey(), message.getValue().getTimestamp());
		return (KVMessageItem) server.getPersistenceLogic().put(keyForVersion, message.getValue());
	}

	public KVMessageItem get(KVMessageItem message) {
		if (server.getVersionController().getKeyForVersion(message.getKey(), message.getVersion()) == null) return new KVMessageItem(KvStatusType.GET_ERROR);
		return (KVMessageItem) server.getPersistenceLogic()
				.get(server.getVersionController().getKeyForVersion(message.getKey(), message.getVersion()));
	}
	
	private void sendReplication(KVMessageItem message) {
		KVMessageItem putReplication = new KVMessageItem(KvStatusType.PUT_REPLICATION, message.getKey(), message.getValue());
		server.getReplicaCoordinator().put(putReplication);
	}
	
}
