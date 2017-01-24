package test.java.Tests;
import main.java.AntidoteClient.*;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.util.*;
import java.util.ArrayList;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * The Class AntidoteTest.
 */
public class AntidoteTest{
	
	/** The antidote client. */
	AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
	
	/** The bucket. */
	String bucket = nextSessionId();
	
	/** The antidote transaction. */
	AntidoteTransaction antidoteTransaction = new AntidoteTransaction(antidoteClient);  
	
	/**
	 * Next session id.
	 *
	 * @return the string
	 */
	public String nextSessionId() {
		SecureRandom random = new SecureRandom();
	    return new BigInteger(130, random).toString(32);
	}
	
	/**
	 * Commit transaction.
	 */
	@Test(timeout=10000)
	public void commitTransaction() {

		AntidoteCounter counter1old = antidoteClient.readCounter("testCounter5",bucket);
		int oldValue1 = counter1old.getValue();
		AntidoteCounter counter2old = antidoteClient.readCounter("testCounter3",bucket);
		int oldValue2 = counter2old.getValue();
		ByteString descriptor = antidoteTransaction.startTransaction();
		antidoteTransaction.updateCounterTransaction("testCounter5",bucket, 5, descriptor);
		antidoteTransaction.updateCounterTransaction("testCounter3",bucket, 3, descriptor);
		antidoteTransaction.readCounterTransaction("testCounter5",bucket, descriptor);
		antidoteTransaction.readCounterTransaction("testCounter3",bucket, descriptor);
		antidoteTransaction.commitTransaction(descriptor);
		AntidoteCounter counter1new = antidoteClient.readCounter("testCounter5",bucket);
		AntidoteCounter counter2new = antidoteClient.readCounter("testCounter3",bucket);
		int newValue1 = counter1new.getValue();
		assert (newValue1 == oldValue1+5);
		int newValue2 = counter2new.getValue();
		assert (newValue2 == oldValue2+3);
	}	
	
	/**
	 * Inc by 2 test.
	 */
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
	
	/**
	 * Decrement to zero test.
	 */
	@Test(timeout=500)
	public void decrementToZeroTest() {
		AntidoteCounter testCounter = antidoteClient.readCounter("testCounter", bucket);
		testCounter.increment(0-testCounter.getValue());
		testCounter.push();
		assert(testCounter.getValue() == 0); //operation executed locally
		testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(testCounter.getValue() == 0); //operation executed in the data base
	}
	
	/**
	 * Inc by 5 test.
	 */
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
	
	/**
	 * Adds the elem test.
	 */
	@Test(timeout=500)
	public void addElemTest() {
		AntidoteORSet testSet = antidoteClient.readORSet("testSet3", bucket);
		testSet.addElement("element");
		testSet.push();
		assert(testSet.getValueList().contains("element"));
		testSet.readDatabase();
		assert(testSet.getValueList().contains("element"));
	}
	
	/**
	 * Rem elem test.
	 */
	@Test(timeout=500)
	public void remElemTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteORSet testSet = antidoteClient.readORSet("testSet3", bucket);
		testSet.addElement(elements);
		testSet.removeElement("Hi");
		testSet.push();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
		testSet.readDatabase();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	/**
	 * Adds the elems test.
	 */
	@Test(timeout=500)
	public void addElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Wall");
		elements.add("Ball");
		AntidoteRWSet testSet = antidoteClient.readRWSet("testSet1", bucket);
		testSet.addElement(elements);
		testSet.push();
		testSet.readDatabase();
		assert(testSet.getValueList().contains("Wall"));
		assert(testSet.getValueList().contains("Ball"));
	}
	
	/**
	 * Rem elems test.
	 */
	@Test(timeout=500)
	public void remElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteRWSet testSet = antidoteClient.readRWSet("testSet1", bucket);
		testSet.addElement(elements);
		testSet.removeElement(elements);
		testSet.push();
		assert(! testSet.getValueList().contains("Hi"));
		assert(! testSet.getValueList().contains("Bye"));
		testSet.addElement(elements);
		testSet.push();
		testSet.readDatabase();
		assert(testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	/**
	 * Update reg test.
	 */
	@Test(timeout=500)
	public void updateRegTest() {
        AntidoteRegister testReg = antidoteClient.readRegister("testReg", bucket);
        testReg.setValue("hi");
        testReg.setValue("bye");
        testReg.push();
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
	}
	
	/**
	 * Update MV reg test.
	 */
	@Test(timeout=500)
	public void updateMVRegTest() {
		AntidoteMVRegister testReg = antidoteClient.readMVRegister("testMVReg", bucket);
        testReg.setValue("hi");
        testReg.setValue("bye");
        testReg.push();
        assert(testReg.getValueList().contains("bye"));
        assert(! testReg.getValueList().contains("hi"));
	}
	
	/**
	 * Inc int by 1 test.
	 */
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
	
	/**
	 * Dec by 5 test.
	 */
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
	
	/**
	 * Sets the int test.
	 */
	@Test(timeout=500)
	public void setIntTest() {					
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", bucket);
		integer.setValue(42);
		integer.push();
		assert(integer.getValue() == 42);	
		integer.readDatabase();
		assert(integer.getValue() == 42);	
	}

	/**
	 * Counter test.
	 */
	@Test(timeout=1000)
	public void counterTest() {
		String counterKey = "counterKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap12", bucket);
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement(5);
		testMap.update(counterKey, counterUpdate);
		int counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert (counterValue == 5); //local value is 5
		testMap.synchronize();
		AntidoteMapCounterEntry counter = testMap.getCounterEntry(counterKey);
		counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert(counterValue == 5); //increment forwarded to database, then got a new state from database
		counter = testMap.getCounterEntry(counterKey);
		counter.increment(5);
		counter.increment(5);
		assert(counter.getValue() == 15); // two local increments in a row
		counter.synchronize();
		assert(counter.getValue() == 15); // two updates sent to database at the same time and new state received after this
		counter.increment(-15);
		counter.push();
		counter.readDatabase();
		testMap.removeCounter(counterKey);
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Integer test.
	 */
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
		testMap.synchronize();
		innerMap = testMap.getAWMapEntry(mapKey); //overwrite local content with database content
		AntidoteMapIntegerEntry integer = innerMap.getIntegerEntry(integerKey);
		integerValue = integer.getValue();
		assert(integerValue == 5); //increment forwarded to database, then got a new state from database
		integer = innerMap.getIntegerEntry(integerKey);
		integer.increment(5);
		integer.increment(5);
		assert(integer.getValue() == 15); // two local increments in a row
		integer.synchronize();
		assert(integer.getValue() == 15); // two updates sent to database at the same time and new state received after this
		integer.setValue(0);
		integer.synchronize();
		testMap.removeAWMap(mapKey);
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Register test.
	 */
	@Test(timeout=500)
	public void registerTest() {
		String registerKey = "registerKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		String registerValue = testMap.getRegisterEntry(registerKey).getValue();
		assert (registerValue.equals("yes")); //local value is "yes"
		AntidoteMapRegisterEntry register = testMap.getRegisterEntry(registerKey);
		testMap.synchronize();
		registerValue = testMap.getRegisterEntry(registerKey).getValue();
		assert(registerValue.equals("yes")); //update forwarded to database, then got a new state from database
		register = testMap.getRegisterEntry(registerKey);
		register.setValue("no");
		register.setValue("maybe");
		assert(register.getValue().equals("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValue().equals("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.removeRegister(registerKey); 
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Mv register test.
	 */
	@Test(timeout=500)
	public void mvRegisterTest() {
		String registerKey = "mvRegisterKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate registerUpdate = antidoteClient.createMVRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		List<String> registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert (registerValueList.contains("yes")); //local value is "yes"
		AntidoteMapMVRegisterEntry register = testMap.getMVRegisterEntry(registerKey);
		testMap.synchronize();
		registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert(registerValueList.contains("yes")); //update forwarded to database, then got a new state from database
		register = testMap.getMVRegisterEntry(registerKey);
		register.setValue("no");
		register.setValueBS(ByteString.copyFromUtf8("maybe"));
		assert(register.getValueList().contains("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValueList().contains("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.removeMVRegister(registerKey); 
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Or set test.
	 */
	@Test(timeout=500)
	public void orSetTest() {
		String setKey = "orSetKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate setUpdate = antidoteClient.createORSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getORSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteMapORSetEntry set = testMap.getORSetEntry(setKey);
		testMap.synchronize();
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
		set.synchronize();
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.removeElement(set.getValueList());
		set.push();
		testMap.removeORSet(setKey); 
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Rw set test.
	 */
	@Test(timeout=500)
	public void rwSetTest() {
		String setKey = "rwSetKey";
		AntidoteAWMap testMap = antidoteClient.readAWMap("testMapBestMap3", bucket);
		AntidoteMapUpdate setUpdate = antidoteClient.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteMapRWSetEntry set = testMap.getRWSetEntry(setKey);
		testMap.synchronize();
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
		set.synchronize();
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.removeElement(set.getValueList());
		set.push();
		testMap.removeRWSet(setKey);
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Rw set test 2.
	 */
	@Test(timeout=500)
	public void rwSetTest2() {
		String setKey = "rwSetKey";
		AntidoteGMap testMap = antidoteClient.readGMap("testMapBestMap4", bucket);
		AntidoteMapUpdate setUpdate = antidoteClient.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteMapRWSetEntry set = testMap.getRWSetEntry(setKey);
		testMap.synchronize();
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
		set.synchronize();
		assert(! set.getValueList().contains("maybe"));
		assert(set.getValueList().contains("no"));// 3 local updates in a row
		set.removeElement(set.getValueList());
		set.push();
		testMap.push(); // everything set to initial situation
	}
	
	/**
	 * Creates the remove test.
	 */
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
		String gMapKey = "gMapKey";

		AntidoteMapUpdate orSetUpdate = antidoteClient.createORSetAdd("yes");
		AntidoteMapUpdate rwSetUpdate = antidoteClient.createRWSetAdd("yes");
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement();
		AntidoteMapUpdate integerUpdate = antidoteClient.createIntegerIncrement();
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("yes");
		AntidoteMapUpdate mvRegisterUpdate = antidoteClient.createMVRegisterSet("yes");
		AntidoteMapUpdate innerAWMapUpdate = antidoteClient.createAWMapUpdate(orSetKey, orSetUpdate);
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate(orSetKey, orSetUpdate);

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
		awMapUpdate = antidoteClient.createAWMapUpdate(gMapKey, gMapUpdate);
		testMap.update(awMapKey, awMapUpdate);
		
		testMap.synchronize();

		AntidoteMapUpdate orSetRemove = antidoteClient.createMapORSetRemove(orSetKey);
		AntidoteMapUpdate awSetRemove = antidoteClient.createMapRWSetRemove(rwSetKey);
		AntidoteMapUpdate counterRemove = antidoteClient.createMapCounterRemove(counterKey);
		AntidoteMapUpdate integerRemove = antidoteClient.createMapIntegerRemove(integerKey);
		AntidoteMapUpdate registerRemove = antidoteClient.createMapRegisterRemove(registerKey);
		AntidoteMapUpdate mvRegisterRemove = antidoteClient.createMapMVRegisterRemove(mvRegisterKey);
		AntidoteMapUpdate awMapRemove = antidoteClient.createMapAWMapRemove(innerAWMapKey);
		AntidoteMapUpdate gMapRemove = antidoteClient.createMapGMapRemove(gMapKey);

		testMap.update(awMapKey, orSetRemove);
		testMap.update(awMapKey, awSetRemove);
		testMap.update(awMapKey, counterRemove);
		testMap.update(awMapKey, integerRemove);
		testMap.update(awMapKey, registerRemove);
		testMap.update(awMapKey, mvRegisterRemove);
		testMap.update(awMapKey, awMapRemove);
		testMap.update(awMapKey, gMapRemove);
		
		testMap.synchronize();

		AntidoteMapAWMapEntry innerMap = testMap.getAWMapEntry(awMapKey);
		
		assert(innerMap.getEntryList().size()==0);
	}
}
