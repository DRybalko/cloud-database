package common.logic;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class to generate hash value based on MD5 algorithm. Provides method to generate 
 * hash value for two strings.
 */
public class HashGenerator {

	private static MessageDigest messageDigest;

	static {
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.getStackTrace();
		}
	}
	
	public static byte[] generateHashForValues(String value1, String value2) {
		byte[] messageToHash = prepareMessageForHash(value1, value2);
		return hash(messageToHash);
	}
	
	//Method to convert to strings to byte and merge them to one byte array
	private static byte[] prepareMessageForHash(String message1, String message2) {
		byte[] ipInBytes = message1.getBytes();
		byte[] portInBytes = message2.getBytes();
		byte[] mergedMessage = new byte[ipInBytes.length + portInBytes.length];
		System.arraycopy(ipInBytes, 0, mergedMessage, 0, ipInBytes.length);
		System.arraycopy(portInBytes, 0, mergedMessage, ipInBytes.length, portInBytes.length);
		return mergedMessage;
	}
	
	public static byte[] generateHashFor(String value) {
		byte[] messageToHash = value.getBytes();
		return hash(messageToHash);
	}
	
	private static byte[] hash(byte[] value) {
		messageDigest.update(value);
		return messageDigest.digest();
	}
}
