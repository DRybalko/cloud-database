package app_kvEcs.logic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import common.logic.KVServerItem;

/**
 * Repository class reads in all servers from ecs.config and creates KVServerItem object from them.
 * These object are then used in meta data table. It provides public method getAvailableServers(), which
 * returns all servers, that are ready to be initialized and started.
 */
public class Repository {
	
	private List<KVServerItem> availableServers;
	private Logger logger;
	
	public Repository(String fileName) {
		logger = Logger.getRootLogger();
		initializeRepository(fileName);
	}
	
	private void initializeRepository(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			readFileLine(reader);
		} catch (IOException e) {
			logger.error("Error while processing file " + fileName +". " + e.getMessage());
		} finally {
			try {
				if (reader != null) reader.close();
			} catch (IOException e) {
				logger.error("Error while closing reader. " + e.getMessage());
			}
		}	
	}
	
	private void readFileLine(BufferedReader reader) throws IOException {
		availableServers = new LinkedList<KVServerItem>();
		String line;
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split(" ");
			String serverName = tokens[0];
			String ip = tokens[1];
			String port = tokens[2];
			availableServers.add(new KVServerItem(serverName, ip, port));
		}
	}
	
	public List<KVServerItem> getAvailableServers() {
		return availableServers;
	}

}
