package common.logic;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Value {

	private int permission;
	private String date;
	private String value;
	
	public Value(int permission, String date, String value){
		this.permission = permission;
		this.value = value;
		this.date = date;
	}

	public Value (String value){
		this.value = value;
	}
	public int getPermission() {
		return permission;
	}

	public void setPermission(int permission) {
		this.permission = permission;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString(){
		String s  = "";
		s = this.getPermission() + " " + this.getDate() + " " + this.getValue();
		return s; 
	}
//	public static void main(String[] args){
//		Value value = new Value(1, new SimpleDateFormat("yyyy/MM/dd_HH/mm/ss/SS").format(Calendar.getInstance().getTime()), "value");
//		System.out.print(value.getValue());
//	}

}
