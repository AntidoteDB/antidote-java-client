package test.java.Tests;
import main.java.AntidoteClient.*;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.util.*;
import java.util.ArrayList;
import org.junit.Test;
public class AntidoteTest{
	AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
	String bucket = nextSessionId();
	  
	public String nextSessionId() {
		SecureRandom random = new SecureRandom();
	    return new BigInteger(130, random).toString(32);
	}
	
	@Test(timeout=1000) 
	public void incBy2Test() {					
		AntidoteCounter counter = antidoteClient.readCounter("testCounter5", bucket);
		int oldValue = counter.getValue();
		counter.increment();
		counter.increment();
		counter.push();
		int newValue = counter.getValue();
		assert(newValue == oldValue+2);
		counter.readDatabase();
		newValue = counter.getValue();
		assert(newValue == oldValue+2);	
	}
	
	@Test(timeout=500)
	public void decrementToZeroTest() {
		AntidoteCounter testCounter = antidoteClient.readCounter("testCounter", bucket);
		testCounter.increment(0-testCounter.getValue());
		testCounter.push();
		assert(testCounter.getValue() == 0); //operation executed locally
		testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(testCounter.getValue() == 0); //operation executed in the data base
	}
	
	@Test(timeout=500)
	public void incBy5Test(){					
		AntidoteCounter counter = antidoteClient.readCounter("testCounter5", bucket);
		int oldValue = counter.getValue();
		counter.increment(5);
		counter.push();
		int newValue = counter.getValue();
		assert(newValue == oldValue+5);
		counter.readDatabase();
		newValue = counter.getValue();
		assert(newValue == oldValue+5);		
	}
	
	@Test(timeout=500)
	public void addElemTest() {
		AntidoteORSet testSet = antidoteClient.readORSet("testSet3", bucket);
		testSet.add("element");
		testSet.push();
		assert(testSet.getValueList().contains("element"));
		testSet.readDatabase();
		assert(testSet.getValueList().contains("element"));
	}
	
	@Test(timeout=500)
	public void remElemTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteORSet testSet = antidoteClient.readORSet("testSet3", bucket);
		testSet.add(elements);
		testSet.remove("Hi");
		testSet.push();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
		testSet.readDatabase();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	@Test(timeout=500)
	public void addElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Wall");
		elements.add("Ball");
		AntidoteRWSet testSet = antidoteClient.readRWSet("testSet1", bucket);
		testSet.add(elements);
		testSet.push();
		testSet.readDatabase();
		assert(testSet.getValueList().contains("Wall"));
		assert(testSet.getValueList().contains("Ball"));
	}
	
	@Test(timeout=500)
	public void remElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteRWSet testSet = antidoteClient.readRWSet("testSet1", bucket);
		testSet.add(elements);
		testSet.remove(elements);
		testSet.push();
		assert(! testSet.getValueList().contains("Hi"));
		assert(! testSet.getValueList().contains("Bye"));
		testSet.add(elements);
		testSet.push();
		testSet.readDatabase();
		assert(testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	@Test(timeout=500)
	public void updateRegTest() {
        AntidoteRegister testReg = antidoteClient.readRegister("testReg", bucket);
        testReg.set("hi");
        testReg.set("bye");
        testReg.push();
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
	}
	
	@Test(timeout=500)
	public void updateMVRegTest() {
		AntidoteMVRegister testReg = antidoteClient.readMVRegister("testMVReg", bucket);
        testReg.set("hi");
        testReg.set("bye");
        testReg.push();
        assert(testReg.getValueList().contains("bye"));
        assert(! testReg.getValueList().contains("hi"));
	}
	
	@Test(timeout=500)
	public void incIntBy1Test() {
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", bucket);
		int oldValue = integer.getValue();
		integer.increment();
		integer.push();
		int newValue = integer.getValue();
		assert(oldValue+1 == newValue);
		integer.readDatabase();
		newValue = integer.getValue();
		assert(oldValue+1 == newValue);
	}
	
	@Test(timeout=500)
	public void decBy5Test() {
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", bucket);
		int oldValue = integer.getValue();
		integer.increment(-5);
		integer.push();
		int newValue = integer.getValue();
		assert(oldValue-5 == newValue);
		integer.readDatabase();
		newValue = integer.getValue();
		assert(oldValue-5 == newValue);
	}
	
	@Test(timeout=500)
	public void setIntTest() {					
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", bucket);
		integer.setValue(42);
		integer.push();
		assert(integer.getValue() == 42);	
		integer.readDatabase();
		assert(integer.getValue() == 42);	
	}

	@Test(timeout=500)
	public void counterTest() {
		String counterKey = "counterKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap12", bucket);
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement(5);
		testMap.update(counterKey, counterUpdate);
		int counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert (counterValue == 5); //local value is 5
		testMap.readDatabase(); //overwrite local content with database content
		AntidoteMapCounterEntry counter = testMap.getCounterEntry(counterKey);
		assert (counter == null); //increment executed only locally, so map in database doesn't have the counter
		testMap.push();
		testMap.readDatabase();
		counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert(counterValue == 5); //increment forwarded to database, then got a new state from database
		counter = testMap.getCounterEntry(counterKey);
		counter.increment(5);
		counter.increment(5);
		assert(counter.getValue() == 15); // two local increments in a row
		counter.push();
		counter.readDatabase();
		assert(counter.getValue() == 15); // two updates sent to database at the same time and new state received after this
		counter.increment(-15);
		counter.push();
		counter.readDatabase();
		testMap.removeCounter(counterKey);
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=500)
	public void integerTest() {
		String integerKey = "integerKey";
		String mapKey = "mapKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap12", bucket);
		AntidoteMapUpdate integerUpdate = antidoteClient.createIntegerIncrement(5);
		AntidoteMapUpdate mapUpdate = antidoteClient.createAWMapUpdate(integerKey, integerUpdate);
		testMap.update(mapKey, mapUpdate);
		AntidoteMapAWMapEntry innerMap = testMap.getAWMapEntry(mapKey);
		int integerValue = innerMap.getIntegerEntry(integerKey).getValue();
		assert (integerValue == 5); //local value is 5
		testMap.readDatabase();
		innerMap = testMap.getAWMapEntry(mapKey); //overwrite local content with database content
		assert (innerMap == null); //increment executed only locally, so map in database doesn't have the integer
		testMap.push();
		testMap.readDatabase();
		innerMap = testMap.getAWMapEntry(mapKey); //overwrite local content with database content
		AntidoteMapIntegerEntry integer = innerMap.getIntegerEntry(integerKey);
		integerValue = integer.getValue();
		assert(integerValue == 5); //increment forwarded to database, then got a new state from database
		integer = innerMap.getIntegerEntry(integerKey);
		integer.increment(5);
		integer.increment(5);
		assert(integer.getValue() == 15); // two local increments in a row
		integer.push();
		integer.readDatabase();
		assert(integer.getValue() == 15); // two updates sent to database at the same time and new state received after this
		integer.set(0);
		integer.push();
		integer.readDatabase();
		testMap.removeAWMap(mapKey);
		testMap.push(); // everything set to initial situation
	}
	
	@Test
	public void registerTest() {
		String registerKey = "registerKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		String registerValue = testMap.getRegisterEntry(registerKey).getValue();
		assert (registerValue.equals("yes")); //local value is "yes"
		testMap.readDatabase(); //overwrite local content with database content
		AntidoteMapRegisterEntry register = testMap.getRegisterEntry(registerKey);
		assert (register == null); //update executed only locally, so map in database doesn't have the register
		testMap.push();
		testMap.readDatabase();
		registerValue = testMap.getRegisterEntry(registerKey).getValue();
		assert(registerValue.equals("yes")); //update forwarded to database, then got a new state from database
		register = testMap.getRegisterEntry(registerKey);
		register.set("no");
		register.set("maybe");
		assert(register.getValue().equals("maybe")); // two local updates in a row
		register.push();
		register.readDatabase();
		assert(register.getValue().equals("maybe")); // two updates sent to database at the same time, order is preserved
		register.set("");
		register.push();
		testMap.removeRegister(registerKey); 
		testMap.push(); // everything set to initial situation
	}
	
	@Test
	public void mvRegisterTest() {
		String registerKey = "mvRegisterKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate registerUpdate = antidoteClient.createMVRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		List<String> registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert (registerValueList.contains("yes")); //local value is "yes"
		testMap.readDatabase(); //overwrite local content with database content
		AntidoteMapMVRegisterEntry register = testMap.getMVRegisterEntry(registerKey);
		assert (register == null); //update executed only locally, so map in database doesn't have the register
		testMap.push();
		testMap.readDatabase();
		registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert(registerValueList.contains("yes")); //update forwarded to database, then got a new state from database
		register = testMap.getMVRegisterEntry(registerKey);
		register.set("no");
		register.set("maybe");
		assert(register.getValueList().contains("maybe")); // two local updates in a row
		register.push();
		register.readDatabase();
		assert(register.getValueList().contains("maybe")); // two updates sent to database at the same time, order is preserved
		register.set("");
		register.push();
		testMap.removeMVRegister(registerKey); 
		testMap.push(); // everything set to initial situation
	}
	
	@Test
	public void orSetTest() {
		String setKey = "orSetKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate setUpdate = antidoteClient.createORSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getORSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		testMap.readDatabase(); //overwrite local content with database content
		AntidoteMapORSetEntry set = testMap.getORSetEntry(setKey);
		assert (set == null); //increment executed only locally, so map in database doesn't have the set
		testMap.push();
		testMap.readDatabase();
		setValueList = testMap.getORSetEntry(setKey).getValueList();
		assert(setValueList.contains("yes")); //update forwarded to database, then got a new state from database
		set = testMap.getORSetEntry(setKey);
		set.addElement("no");
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements);
		set.removeElement(elements);
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.push();
		set.readDatabase();
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.removeElement(set.getValueList());
		set.push();
		testMap.removeORSet(setKey); 
		testMap.push(); // everything set to initial situation
	}
	
	@Test
	public void rwSetTest() {
		String setKey = "rwSetKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate setUpdate = antidoteClient.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		testMap.readDatabase(); //overwrite local content with database content
		AntidoteMapRWSetEntry set = testMap.getRWSetEntry(setKey);
		assert (set == null); //increment executed only locally, so map in database doesn't have the set
		testMap.push();
		testMap.readDatabase();
		setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert(setValueList.contains("yes")); //update fRWwarded to database, then got a new state from database
		set = testMap.getRWSetEntry(setKey);
		set.addElement("no");
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements);
		set.removeElement(elements);
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.push();
		set.readDatabase();
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.removeElement(set.getValueList());
		set.push();
		testMap.removeRWSet(setKey);
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=500)
	public void createRemoveTest() {
		AntidoteAWMap testMap = antidoteClient.readAWMap("emptyMapBestMap", bucket);
		
		String orSetKey = "tempORSetKey";
		String rwSetKey = "tempRWSetKey";
		String counterKey = "tempCounterKey";
		String integerKey = "tempIntegerKey";
		String registerKey = "tempRegisterKey";
		String mvRegisterKey = "tempMVRegisterKey";
		String innerAWMapKey = "tempAWMapKey";
		String awMapKey = "awMapKey";

		AntidoteMapUpdate orSetUpdate = antidoteClient.createORSetAdd("yes");
		AntidoteMapUpdate rwSetUpdate = antidoteClient.createRWSetAdd("yes");
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement();
		AntidoteMapUpdate integerUpdate = antidoteClient.createIntegerIncrement();
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("yes");
		AntidoteMapUpdate mvRegisterUpdate = antidoteClient.createMVRegisterSet("yes");
		AntidoteMapUpdate innerAWMapUpdate = antidoteClient.createAWMapUpdate(orSetKey, orSetUpdate);

		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate(orSetKey, orSetUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(rwSetKey, rwSetUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(counterKey, counterUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(integerKey, integerUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(registerKey, registerUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(mvRegisterKey, mvRegisterUpdate);
		testMap.update(awMapKey, awMapUpdate);
		awMapUpdate = antidoteClient.createAWMapUpdate(innerAWMapKey, innerAWMapUpdate);
		testMap.update(awMapKey, awMapUpdate);
		
		testMap.push();
		testMap.readDatabase();

		AntidoteMapUpdate orSetRemove = antidoteClient.createMapORSetRemove(orSetKey);
		AntidoteMapUpdate awSetRemove = antidoteClient.createMapRWSetRemove(rwSetKey);
		AntidoteMapUpdate counterRemove = antidoteClient.createMapCounterRemove(counterKey);
		AntidoteMapUpdate integerRemove = antidoteClient.createMapIntegerRemove(integerKey);
		AntidoteMapUpdate registerRemove = antidoteClient.createMapRegisterRemove(registerKey);
		AntidoteMapUpdate mvRegisterRemove = antidoteClient.createMapMVRegisterRemove(mvRegisterKey);
		AntidoteMapUpdate awMapRemove = antidoteClient.createMapAWMapRemove(innerAWMapKey);

		testMap.update(awMapKey, orSetRemove);
		testMap.update(awMapKey, awSetRemove);
		testMap.update(awMapKey, counterRemove);
		testMap.update(awMapKey, integerRemove);
		testMap.update(awMapKey, registerRemove);
		testMap.update(awMapKey, mvRegisterRemove);
		testMap.update(awMapKey, awMapRemove);
		
		testMap.push();
		testMap.readDatabase();

		AntidoteMapAWMapEntry innerMap = testMap.getAWMapEntry(awMapKey);
		
		assert(innerMap.getEntryList().size()==0);
	}
}
