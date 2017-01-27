package common.messages.ecsToServerMessage;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import common.logic.KVServerItem;
import common.messages.Message.MessageType;
import common.messages.ecsToServerMessage.ECSMessage.EcsStatusType;

public class ECSMessageMarshaller {

	private static final String UNIT_SEPARATOR = "9c3V%_"; 
	private static final char CARRIAGE = (char) 13;
	private static final String ATTRIBUTE_SEPARATOR = "a2Lv-`;1"; 
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	public static byte[] marshal(ECSMessage message) {
		StringBuilder stringBuilder = new StringBuilder();	
		stringBuilder.append(MessageType.ECS_TO_SERVER);
		stringBuilder.append(UNIT_SEPARATOR);
		stringBuilder.append(message.getStatus().toString());
		stringBuilder.append(UNIT_SEPARATOR);
		if (message.getStatus().equals(EcsStatusType.META_DATA_TABLE)) {
			stringBuilder.append(convertMetaDataTableToString(message.getMetaDataTable()));
		} else if (message.getStatus().equals(EcsStatusType.UPDATE_START_INDEX)) {
			stringBuilder.append(new String(message.getStartIndex(), CHARSET));
		} else if (message.getStatus().equals(EcsStatusType.SERVER_START_END_INDEX)) {
			stringBuilder.append(new String(message.getStartIndex(), CHARSET));
			stringBuilder.append(UNIT_SEPARATOR);
			stringBuilder.append(new String(message.getEndIndex(), CHARSET));
		} else if (message.getStatus().equals(EcsStatusType.FAULTY_SERVER) 
				|| message.getStatus().equals(EcsStatusType.ECS_METADATA)) {
			stringBuilder.append(convertKVServerItemToString(message.getServerItem()));
		}
		stringBuilder.append(CARRIAGE);
		return stringBuilder.toString().getBytes(CHARSET);
	};

	private static String convertMetaDataTableToString(List<KVServerItem> metaDataTable) {
		StringBuilder stringBuilder = new StringBuilder();
		ListIterator<KVServerItem> iterator = metaDataTable.listIterator();
		while (iterator.hasNext()) {
			String serverString = convertKVServerItemToString(iterator.next());
			stringBuilder.append(serverString);
			if (iterator.hasNext()) stringBuilder.append(UNIT_SEPARATOR);
		}
		return stringBuilder.toString();
	}

	public static ECSMessageItem unmarshal(String[] messageTokens){
		ECSMessageItem messageItem = new ECSMessageItem(EcsStatusType.valueOf(messageTokens[0]));
		messageItem.setMessageType(MessageType.ECS_TO_SERVER);
		if (messageTokens[0].equals(EcsStatusType.META_DATA_TABLE.toString())) {
			messageItem.setMetaDataTable(convertStringToMetaDataTable(messageTokens));
		} else if (messageTokens[0].equals(EcsStatusType.SERVER_START_END_INDEX.toString())) {
			messageItem.setStartIndex(messageTokens[1].getBytes(CHARSET));
			messageItem.setEndIndex(messageTokens[2].getBytes(CHARSET));
		} else if (messageTokens[0].equals(EcsStatusType.UPDATE_START_INDEX.toString())) {
			messageItem.setStartIndex(messageTokens[1].getBytes(CHARSET));
		} else if (messageTokens[0].equals(EcsStatusType.FAULTY_SERVER.toString()) 
				|| messageTokens[0].equals(EcsStatusType.ECS_METADATA.toString())) {
			messageItem.setServerItem(convertStringToMetaDataTableServer(messageTokens[1]));
		}
		return messageItem;
	}	

	private static List<KVServerItem> convertStringToMetaDataTable(String[] messageTokens)  {
		List<KVServerItem> metaDataTable = new ArrayList<>();
		for (int i=1; i<messageTokens.length; i++) {
			KVServerItem kvServer = convertStringToMetaDataTableServer(messageTokens[i]);
			metaDataTable.add(kvServer);
		}
		return metaDataTable;
	}

	protected static String convertKVServerItemToString(KVServerItem server) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(server.getName());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getIp());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(server.getPort());
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getStartIndex(), CHARSET));
		stringBuilder.append(ATTRIBUTE_SEPARATOR);
		stringBuilder.append(new String(server.getEndIndex(), CHARSET));
		return stringBuilder.toString();
	}

	protected static KVServerItem convertStringToMetaDataTableServer(String message)  {
		String[] serverTokens = message.split(ATTRIBUTE_SEPARATOR);
		KVServerItem kvServer = new KVServerItem(serverTokens[0], serverTokens[1], serverTokens[2]);
		kvServer.setStartIndex(serverTokens[3].getBytes(CHARSET));
		kvServer.setEndIndex(serverTokens[4].getBytes(CHARSET));
		return kvServer;
	}

}
