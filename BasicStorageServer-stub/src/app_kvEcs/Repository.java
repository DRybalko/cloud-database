package app_kvEcs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class Repository {
	
	private Set<KVServerItem> availableServers;
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
		availableServers = new HashSet<KVServerItem>();
		String line;
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split(" ");
			String serverName = tokens[0];
			String ip = tokens[1];
			String port = tokens[2];
			availableServers.add(new KVServerItem(serverName, ip, port));
		}
	}
	
	public Set<KVServerItem> getAvailableServers() {
		return availableServers;
	}

}
