package test.java.Tests;
import main.java.AntidoteClient.*;

import java.util.*;
import java.util.ArrayList;
import org.junit.Test;
public class AntidoteTest{
	AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
	
	@Test
	public void incBy1Test() {					
		AntidoteCounter counter = antidoteClient.readCounter("testCounter5", "testBucket");
		int oldValue = counter.getValue();
		counter.increment();
		int newValue = counter.getValue();
		assert(newValue == oldValue+1);
		counter.getUpdate();
		newValue = counter.getValue();
		assert(newValue == oldValue+1);	
	}
	
	@Test
	public void decrementToZeroTest() {
		AntidoteCounter testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		testCounter.increment(0-testCounter.getValue());
		assert(testCounter.getValue() == 0); //operation executed locally
		testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(testCounter.getValue() == 0); //operation executed in the data base
	}
	
	@Test
	public void incBy5Test(){					
		AntidoteCounter counter = antidoteClient.readCounter("testCounter5", "testBucket");
		int oldValue = counter.getValue();
		counter.increment(5);
		int newValue = counter.getValue();
		assert(newValue == oldValue+5);
		counter.getUpdate();
		newValue = counter.getValue();
		assert(newValue == oldValue+5);		
	}
	
	@Test
	public void notInitializedTest(){
		AntidoteCounter notInitCounter = antidoteClient.readCounter("someCounter2", "someBucket2");  //counter not initialized is always 0
		assert(notInitCounter.getValue() == 0);		
	}
	
	@Test
	public void addElemTest() {
		AntidoteSet testSet = antidoteClient.readSet("testSet", "testBucket");
		testSet.add("element");
		assert(testSet.getValueList().contains("element"));
		testSet.getUpdate();
		assert(testSet.getValueList().contains("element"));
	}
	
	@Test
	public void addElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteSet testSet = antidoteClient.readSet("testSet", "testBucket");
		testSet.add(elements);
		assert(testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
		testSet.getUpdate();
		assert(testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	@Test
	public void remElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteSet testSet = antidoteClient.readSet("testSet", "testBucket");
		testSet.add(elements);
		testSet.remove(elements);
		assert(! testSet.getValueList().contains("Hi"));
		assert(! testSet.getValueList().contains("Bye"));
		testSet.getUpdate();
		assert(! testSet.getValueList().contains("Hi"));
		assert(! testSet.getValueList().contains("Bye"));
	}
	
	@Test
	public void remElemTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteSet testSet = antidoteClient.readSet("testSet", "testBucket");
		testSet.add(elements);
		testSet.remove("Hi");
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
		testSet.getUpdate();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	@Test
	public void updateRegTest() {
        AntidoteRegister testReg = antidoteClient.readRegister("testReg", "testBucket");
        testReg.update("hi");
        testReg.update("bye");
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
	}
	
	@Test
	public void updateMVRegTest() {
		AntidoteMVRegister testReg = antidoteClient.readMVRegister("testMVReg", "testBucket");
        testReg.update("hi");
        testReg.update("bye");
        assert(testReg.getValueList().contains("bye"));
        assert(! testReg.getValueList().contains("hi"));
	}
	
	@Test
	public void notInitializedReg(){
		AntidoteMVRegister testReg = antidoteClient.readMVRegister("someReg", "someBucket");
        assert(testReg.getValueList().isEmpty());
	}
	
	@Test
	public void incIntBy1Test() {
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", "testBucket");
		int oldValue = integer.getValue();
		integer.increment();
		int newValue = integer.getValue();
		assert(oldValue+1 == newValue);
		integer.getUpdate();
		newValue = integer.getValue();
		assert(oldValue+1 == newValue);
	}
	
	@Test
	public void decBy5Test() {
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", "testBucket");
		int oldValue = integer.getValue();
		integer.increment(-5);
		int newValue = integer.getValue();
		assert(oldValue-5 == newValue);
		integer.getUpdate();
		newValue = integer.getValue();
		assert(oldValue-5 == newValue);
	}
	
	@Test
	public void setIntTest() {					
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", "testBucket");
		integer.setValue(42);
		assert(integer.getValue() == 42);	
		integer.getUpdate();
		assert(integer.getValue() == 42);	
	}

	@Test
	public void counterTest() {
		AntidoteMap testMap = antidoteClient.readMap("testMap6123", "testBucket");
		AntidoteMapUpdate counterUpdate = testMap.createCounterIncrement(5);
		try {
			testMap.update("counterkey", counterUpdate);
			testMap.update("counterkey2", counterUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		testMap.getUpdate();
		testMap.removeCounter("counterkey");
		testMap.getUpdate();
		AntidoteMapUpdate mapUpdate = null;
		try {
			mapUpdate = testMap.createMapUpdate("testCounter", counterUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			testMap.update("mapKey", mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AntidoteMapMapEntry nestedMap = testMap.getMapEntry("mapKey");
		AntidoteMapCounterEntry counter2 = nestedMap.getCounterEntry("testCounter");
		int oldValue = counter2.getValue();
		counter2.increment(20);
		counter2.getUpdate();
		int newValue = counter2.getValue();
		assert(newValue == oldValue +20);
		try {
			nestedMap.update("testCounter", counterUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		counter2.getUpdate();
		oldValue = newValue;
		newValue = counter2.getValue();
		assert(oldValue + 5 == newValue);
		nestedMap.removeCounter("testCounter");
		AntidoteMapCounterEntry counter = testMap.getCounterEntry("counterkey2");
		oldValue = counter.getValue();
		counter.increment(50);
		counter.getUpdate();
		newValue = counter.getValue();
		assert(newValue == oldValue +50);
	}
	
	@Test
	public void integerTest() {
		AntidoteMap testMap = antidoteClient.readMap("testMap612", "testBucket");
		AntidoteMapUpdate integerUpdate = testMap.createIntegerIncrement(5);
		try {
			testMap.update("integerkey", integerUpdate);
			testMap.update("integerkey2", integerUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		testMap.getUpdate();
		testMap.removeInteger("integerkey");
		testMap.getUpdate();
		AntidoteMapUpdate mapUpdate = null;
		try {
			mapUpdate = testMap.createMapUpdate("testInteger", integerUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			testMap.update("mapKey", mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AntidoteMapMapEntry nestedMap = testMap.getMapEntry("mapKey");
		AntidoteMapIntegerEntry integer2 = nestedMap.getIntegerEntry("testInteger");
		int oldValue = integer2.getValue();
		integer2.increment(20);
		integer2.getUpdate();
		int newValue = integer2.getValue();
		assert(newValue == oldValue +20);
		try {
			nestedMap.update("testInteger", integerUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		integer2.getUpdate();
		oldValue = newValue;
		newValue = integer2.getValue();
		assert(oldValue + 5 == newValue);
		nestedMap.removeInteger("testInteger");
		AntidoteMapIntegerEntry integer = testMap.getIntegerEntry("integerkey2");
		oldValue = integer.getValue();
		integer.increment(50);
		integer.getUpdate();
		newValue = integer.getValue();
		assert(newValue == oldValue +50);
	}
	
	@Test
	public void setTest() {
		AntidoteMap testMap = antidoteClient.readMap("testMap25", "testBucket");
		AntidoteMapUpdate setUpdate = testMap.createSetAdd("hi");
		AntidoteMapUpdate mapUpdate = null;
		try {
			AntidoteMapUpdate mapUpdate3 = testMap.createMapUpdate("testSet", setUpdate);
			AntidoteMapUpdate mapUpdate2 = testMap.createMapUpdate("testMapNested3", mapUpdate3);
			mapUpdate = testMap.createMapUpdate("testMapNested2", mapUpdate2);
			System.out.println(mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			testMap.update("testMapNested1", mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AntidoteMapMapEntry nestedMap1 = testMap.getMapEntry("testMapNested1");
		AntidoteMapMapEntry nestedMap2 = nestedMap1.getMapEntry("testMapNested2");
		AntidoteMapMapEntry nestedMap3 = nestedMap2.getMapEntry("testMapNested3");
		AntidoteMapSetEntry set = nestedMap3.getSetEntry("testSet");
		System.out.println(set.getValueList());
		assert(set.getValueList().contains("hi"));
		nestedMap3.removeSet("testSet");
		nestedMap3.getUpdate();
		assert(nestedMap3.getEntryList().size()==0);
	}
	
	@Test
	public void registerTest() {
		AntidoteMap testMap = antidoteClient.readMap("testMap22", "testBucket");
		AntidoteMapUpdate regUpdate = testMap.createRegisterSet("hi");
		AntidoteMapUpdate mapUpdate = null;
		try {
			AntidoteMapUpdate mapUpdate3 = testMap.createMapUpdate("testReg", regUpdate);
			AntidoteMapUpdate mapUpdate2 = testMap.createMapUpdate("testMapNested3", mapUpdate3);
			mapUpdate = testMap.createMapUpdate("testMapNested2", mapUpdate2);
			System.out.println(mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			testMap.update("testMapNested1", mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AntidoteMapMapEntry nestedMap1 = testMap.getMapEntry("testMapNested1");
		AntidoteMapMapEntry nestedMap2 = nestedMap1.getMapEntry("testMapNested2");
		AntidoteMapMapEntry nestedMap3 = nestedMap2.getMapEntry("testMapNested3");
		AntidoteMapRegisterEntry register = nestedMap3.getRegisterEntry("testReg");
		assert(register.getValue().equals("hi"));
		regUpdate = testMap.createRegisterSet("bus");
		try {
			nestedMap3.update("testReg", regUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		};
		register.getUpdate();
		assert(register.getValue().equals("bus"));
		register.update("car");
		register.getUpdate();
		assert(register.getValue().equals("car"));
	}
	
	@Test
	public void mvRegisterTest() {
		AntidoteMap testMap = antidoteClient.readMap("testMap23", "testBucket");
		AntidoteMapUpdate regUpdate = testMap.createMVRegisterSet("hi");
		AntidoteMapUpdate mapUpdate = null;
		try {
			AntidoteMapUpdate mapUpdate3 = testMap.createMapUpdate("testReg", regUpdate);
			AntidoteMapUpdate mapUpdate2 = testMap.createMapUpdate("testMapNested3", mapUpdate3);
			mapUpdate = testMap.createMapUpdate("testMapNested2", mapUpdate2);
			System.out.println(mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			testMap.update("testMapNested1", mapUpdate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AntidoteMapMapEntry nestedMap1 = testMap.getMapEntry("testMapNested1");
		AntidoteMapMapEntry nestedMap2 = nestedMap1.getMapEntry("testMapNested2");
		AntidoteMapMapEntry nestedMap3 = nestedMap2.getMapEntry("testMapNested3");
		AntidoteMapMVRegisterEntry register = nestedMap3.getMVRegisterEntry("testReg");
		assert(register.getValueList().contains("hi"));
		register.update("car");
		assert(register.getValueList().contains("car"));
	}
}
