package common.logic;

import java.util.Arrays;

/** Class used for performing increment and decrement for array of bytes as well as comparing
 * two arrays and comparing with null. The result of all operation is returned as new array.
 * Input array is not changed!  
 * 
 * Byte basics in java. Byte in java can only be signed. See examples:
 * (byte) -1 -> 0b11111111
 * (byte) -128 -> 0b1000000
 * (byte) 127 -> 0b01111111
 */
public class ByteArrayMath {

	public static byte[] decrement(byte[] number) {
		byte[] result = Arrays.copyOf(number, number.length);
		return subtractOne(result, result.length - 1);
	}

	private static byte[] subtractOne(byte[] number, int index) {
		if (index < 0) return setForAll(number, (byte) -1);
		else if (number[index] == 0) {
			number[index]--;
			return subtractOne(number, index - 1);
		} else {
			number[index]--;
			return number;
		}
	}
	
	private static byte[] setForAll(byte[] number, byte numberToSet) {
		for (int i = 0; i < number.length; i++) {
			number[i] = numberToSet;
		}
		return number;
	}
	
	public static byte[] increment(byte[] number) {
		byte[] result = Arrays.copyOf(number, number.length);
		return addOne(result, result.length - 1);
	}
	
	private static byte[] addOne(byte[] number, int index) {
		if (index == 0) return setForAll(number, (byte) 0); 
		else if (number[index] == -1){
			number[index]++;
			return addOne(number, index - 1);
		} else {
			number[index]++;
			return number;
		}
	}
	
	/** Compares two arrays with bytes, starting with 0th array element
	 * 
	 * @param number1 to compare
	 * @param number2 to compare
	 * @return  < 0 if number1 < number 2
	 * 			> 0 if number1 > number 2
	 * 			0 	if number1 == number 2
	 */
	public static int compareByteArrays(byte[] number1, byte[] number2) {
		for (int i = 0; i < number1.length; i++) {
			if (number1[i] != number2[i]) {
				return (number1[i] & 0xFF) - (number2[i] & 0xFF);
			}
		}
		return 0;
	}
	
	public static int compareWithNull(byte[] number) {
		return compareByteArrays(number, setForAll(new byte[number.length], (byte) 0));
	}

	
	public static boolean isValueBetweenTwoOthers(byte[] newValue, byte[] startValue, byte[] endValue) {
		if (ByteArrayMath.compareByteArrays(startValue, endValue) < 0) {
			return ByteArrayMath.compareByteArrays(newValue, startValue) > 0
					&& ByteArrayMath.compareByteArrays(newValue, endValue) < 0;
		} else {
			return ByteArrayMath.compareByteArrays(newValue, startValue) > 0 
					|| ByteArrayMath.compareByteArrays(newValue, endValue) < 0;
		}
	}
}
