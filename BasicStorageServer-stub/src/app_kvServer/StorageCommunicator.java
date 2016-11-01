package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class StorageCommunicator {

	private static final String FILE_PATH = "storage/storage.txt";
	private final String SEPARATOR = ":";
	
	private BufferedReader bufferedReader;
	private BufferedWriter bufferedWriter;
	private Logger logger;
	
	public StorageCommunicator() {
		initialize();
		logger = Logger.getRootLogger();
	}
	
	private void initialize() {
		try {
			FileWriter fileWriter = new FileWriter(FILE_PATH, true);
			bufferedWriter = new BufferedWriter(fileWriter);
			FileReader fileReader = new FileReader(FILE_PATH);
			bufferedReader = new BufferedReader(fileReader);
		} catch (FileNotFoundException e) {
			logger.error("File was not found: " + e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void write(String key, String value) {
		String lineToWrite = key + ":" + value;
		try {
			bufferedWriter.write(lineToWrite);
			bufferedWriter.newLine();
			bufferedWriter.flush();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public String readValueFor(String key) {
		//create new file reader to start reading from the first lin
		try {
			FileReader fileReader = new FileReader(FILE_PATH);
			bufferedReader = new BufferedReader(fileReader);
		} catch (FileNotFoundException e) {
			logger.error("File was not found: " + e.getMessage());
		}
		String[] lineTokens = new String[2];
		try {
			return findLine(key);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return lineTokens[1];
	}
	
	private String findLine(String key) throws IOException {
		while (true) {
			String lineFromFile = bufferedReader.readLine();
			if (!(lineFromFile == null || lineFromFile.equals(""))) {
				String[] tokens = lineFromFile.split(SEPARATOR);
				String keyFromFile = tokens[0];
				if (keyFromFile.equals(key)) return tokens[1];
			} else {
				return null;
			}
		}	
	}

}
