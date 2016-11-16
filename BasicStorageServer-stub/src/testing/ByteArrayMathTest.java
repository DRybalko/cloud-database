package testing;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import common.logic.ByteArrayMath;

public class ByteArrayMathTest {

	@Test
	public void testAddOneToPrimitive() {
		byte[] number = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 9, -1, -1, -1};
		byte[] result = ByteArrayMath.increment(number);
		byte[] resultToCompare = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 10, 0, 0, 0};
		assertTrue(Arrays.equals(result, resultToCompare));
	}
	

	@Test
	public void testAddOneToMax() {
		byte[] number = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
		byte[] result = ByteArrayMath.increment(number);
		byte[] resultToCompare = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		assertTrue(Arrays.equals(result, resultToCompare));
	}
	
	@Test
	public void testSubtractOneFromPrimitive() {
		byte[] number = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 9, 0, 0, 0};
		byte[] result = ByteArrayMath.decrement(number);
		byte[] resultToCompare = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 8, -1, -1, -1};
		assertTrue(Arrays.equals(result, resultToCompare));
	}
	
	@Test
	public void testSubtractOneFromMin() {
		byte[] number = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		byte[] result = ByteArrayMath.decrement(number);
		byte[] resultToCompare = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
		assertTrue(Arrays.equals(result, resultToCompare));
	}
	
	@Test
	public void testCompareByteArraysUnequalWithNegtives() {
		byte[] number1 = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 9, 127, 127, 127};
		byte[] number2 = new byte[]{3, 123, -5, 127, 121, 2, 64, 70, 53, 12, 32, 53, 9, 127, 127, 127};
		assertTrue(ByteArrayMath.compareByteArrays(number1, number2) < 0);
	}

	@Test
	public void testCompareByteArraysWithPositives() {
		byte[] number1 = new byte[]{3, 123, 53, 127, 121, 54, 64, 75, 53, 12, 32, 53, 9, 127, 127, 127};
		byte[] number2 = new byte[]{3, 123, 53, 127, 121, 54, 64, 70, 53, 12, 32, 53, 9, 127, 127, 127};
		assertTrue(ByteArrayMath.compareByteArrays(number1, number2) > 0);
	}
	
	@Test
	public void testCompareByteArraysWithNegatives() {
		byte[] number1 = new byte[]{3, 123, 53, 127, -19, 54, 64, 75, 53, 12, 32, 53, 9, 127, 127, 127};
		byte[] number2 = new byte[]{3, 123, 53, 127, -40, 54, 64, 70, 53, 12, 32, 53, 9, 127, 127, 127};
		assertTrue(ByteArrayMath.compareByteArrays(number1, number2) > 0);
	}
	
	@Test
	public void testCompareByteArraysEqual() {
		byte[] number1 = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 9, 127, 127, 127};
		byte[] number2 = new byte[]{3, 123, 53, 127, 121, 54, 64, 21, 53, 12, 32, 53, 9, 127, 127, 127};
		assertTrue(ByteArrayMath.compareByteArrays(number1, number2) == 0);
	}
}
