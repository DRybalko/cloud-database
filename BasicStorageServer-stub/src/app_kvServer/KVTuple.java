package app_kvServer;

import common.logic.Value;

/**
 * This class provides a tuple (<key>,<value>) structure. 
 *
 */

public class KVTuple {

	private String key;
	private Value value;
	
	public KVTuple(String key, Value value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}
	
}
