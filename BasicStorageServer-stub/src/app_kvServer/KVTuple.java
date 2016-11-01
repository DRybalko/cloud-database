package app_kvServer;

/**
 * This class provides a tuple (<key>,<value>) structure. 
 *
 */

public class KVTuple {

	private String key;
	private String value;
	
	public KVTuple(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
