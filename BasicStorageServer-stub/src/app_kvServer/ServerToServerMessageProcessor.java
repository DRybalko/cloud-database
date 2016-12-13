package app_kvServer;

import org.apache.log4j.Logger;

import common.messages.PingMessage.PingStatusType;
import common.messages.PingMessageItem;

public class ServerToServerMessageProcessor {

	private KVServer server;
	private Logger logger;
	
	public ServerToServerMessageProcessor(KVServer server) {
		this.server = server;
		this.logger = Logger.getRootLogger();
	}
	
	public PingMessageItem processMessage(PingMessageItem message) {
		if (message.getStatus().equals(PingStatusType.GET_STATUS)){
			logger.debug("Got Ping message from another server");
		} 
		return new PingMessageItem(PingStatusType.IN_PROGRESS);
	}
}
