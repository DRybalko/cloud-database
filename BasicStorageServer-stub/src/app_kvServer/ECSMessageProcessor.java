package app_kvServer;

import org.apache.log4j.Logger;

import common.messages.ecsToServerMessage.ECSMessageItem;
import common.messages.ecsToServerMessage.ECSMessage.EcsStatusType;

/**
 * Messages, that are received from ECS are processed in this class. First the message status type
 * is identified. After that the corresponding server method is called.
 */
public class ECSMessageProcessor {

	private KVServer server;
	private Logger logger;
	
	public ECSMessageProcessor(KVServer server) {
		this.server = server;
		this.logger = Logger.getRootLogger();
	}
	
	public ECSMessageItem processMessage(ECSMessageItem message) {
		if (message.getStatus().equals(EcsStatusType.START)){
			server.start();
			logger.debug("Got ECS request to start server. Server started");
		} else if (message.getStatus().equals(EcsStatusType.SERVER_START_END_INDEX)) {
			logger.debug("ECS request to initialize start and end index");
			server.setStartIndex(message.getStartIndex());
			server.setEndIndex(message.getEndIndex());
		} else if (message.getStatus().equals(EcsStatusType.META_DATA_TABLE)) {
			logger.debug("Receive meta data table");
			server.setMetaDataTable(message.getMetaDataTable());
		} else if (message.getStatus().equals(EcsStatusType.STOP)) {
			server.stop();
		} else if (message.getStatus().equals(EcsStatusType.SHUT_DOWN)) {
			server.shutDown();
		} else if (message.getStatus().equals(EcsStatusType.UPDATE_START_INDEX)) {
			server.updateStartIndex(message.getStartIndex());
		} else if (message.getStatus().equals(EcsStatusType.ECS_METADATA)) {
			server.setEcsMetaData(message.getServerItem());
		}
		return new ECSMessageItem(EcsStatusType.REQUEST_ACCEPTED);
	}
	
}
