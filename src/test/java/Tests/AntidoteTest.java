package test.java.Tests;
import main.java.AntidoteClient.*;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.util.*;
import java.util.ArrayList;
import org.junit.Test;
import com.google.protobuf.ByteString;

public class AntidoteTest{

	PoolManager antidotePoolManager;
	AntidoteClient antidoteClient;
	String bucket;
	AntidoteTransaction antidoteTransaction;

	public AntidoteTest() {
		List<Host> hosts = new LinkedList<Host>();
		hosts.add(new Host("192.168.99.100", 8087));
		hosts.add(new Host("localhost", 8087));
		antidotePoolManager = new PoolManager(20, 5, hosts);
		antidoteClient = new AntidoteClient(antidotePoolManager);
		bucket = nextSessionId();
		antidoteTransaction = new AntidoteTransaction(antidoteClient);
	}
	
	public String nextSessionId() {
		SecureRandom random = new SecureRandom();
	    return new BigInteger(130, random).toString(32);
	}
	
	@Test(timeout=10000)
	public void commitTransaction() {
		AntidoteCounter counter1old = antidoteClient.readCounter("testCounter5",bucket);
		int oldValue1 = counter1old.getValue();
		AntidoteCounter counter2old = antidoteClient.readCounter("testCounter3",bucket);
		int oldValue2 = counter2old.getValue();
		ByteString descriptor = antidoteTransaction.startTransaction();
		antidoteTransaction.updateCounterTransaction("testCounter5",bucket, 5, descriptor);
		antidoteTransaction.updateCounterTransaction("testCounter3",bucket, 3, descriptor);
		AntidoteCounter counter1new = antidoteTransaction.readCounterTransaction("testCounter5",bucket, descriptor);
		AntidoteCounter counter2new = antidoteTransaction.readCounterTransaction("testCounter3",bucket, descriptor);
		antidoteTransaction.commitTransaction(descriptor);
		int newValue1 = counter1new.getValue();
		assert (newValue1 == oldValue1+5);
		int newValue2 = counter2new.getValue();
		assert (newValue2 == oldValue2+3);
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
		testSet.addElement("element");
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
		testSet.addElement(elements);
		testSet.removeElement("Hi");
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
		testSet.addElement(elements);
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
	
	@Test(timeout=500)
	public void updateRegTest() {
        AntidoteRegister testReg = antidoteClient.readRegister("testReg", bucket);
        testReg.setValue("hi");
        testReg.setValue("bye");
        testReg.push();
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
	}
	
	@Test(timeout=500)
	public void updateMVRegTest() {
		AntidoteMVRegister testReg = antidoteClient.readMVRegister("testMVReg", bucket);
        testReg.setValue("hi");
        testReg.setValue("bye");
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
	
	@Test(timeout=500)
	public void rwSetTest4() {
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
	
	@Test(timeout=500)
	public void updateTest() {
		antidoteClient.updateCounter("testCounter", bucket, 2);
		antidoteClient.addORSetElement("testORSet", bucket, "Hi");
		antidoteClient.addRWSetElement("testRWSet", bucket, "Hi");
		antidoteClient.addORSetElementBS("testORSet", bucket, ByteString.copyFromUtf8("Hi2"));
		antidoteClient.addRWSetElementBS("testRWSet", bucket, ByteString.copyFromUtf8("Hi2"));
		antidoteClient.addORSetElement("testORSet", bucket, "Hi3");
		antidoteClient.addRWSetElement("testRWSet", bucket, "Hi3");
		antidoteClient.removeORSetElementBS("testORSet", bucket, ByteString.copyFromUtf8("Hi"));
		antidoteClient.removeRWSetElementBS("testRWSet", bucket, ByteString.copyFromUtf8("Hi"));
		antidoteClient.removeORSetElement("testORSet", bucket, "Hi3");
		antidoteClient.removeRWSetElement("testRWSet", bucket, "Hi3");
		antidoteClient.setInteger("testInteger", bucket, 7);
		antidoteClient.incrementInteger("testInteger", bucket, 1);
		antidoteClient.updateRegister("testRegister", bucket, "Hi");
		antidoteClient.updateMVRegister("testMVRegister", bucket, "Hi");
		
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.RegisterType, "testRegister");
		AntidoteMapKey mvRegisterKey = new AntidoteMapKey(AntidoteType.MVRegisterType, "testMVRegister");

		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement();
		AntidoteMapUpdate intSet = antidoteClient.createIntegerSet(3);
		AntidoteMapUpdate intInc = antidoteClient.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetAdd = antidoteClient.createRWSetAdd("Hi");
		AntidoteMapUpdate rwSetAdd2 = antidoteClient.createRWSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate rwSetAdd3 = antidoteClient.createRWSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate orSetAdd = antidoteClient.createORSetAdd("Hi");
		AntidoteMapUpdate orSetAdd2 = antidoteClient.createORSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetAdd3 = antidoteClient.createORSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate rwSetRemove = antidoteClient.createRWSetRemove("Hi");
		AntidoteMapUpdate rwSetRemove2 = antidoteClient.createRWSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetRemove = antidoteClient.createORSetRemove("Hi");
		AntidoteMapUpdate orSetRemove2 = antidoteClient.createORSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("Hi");
		AntidoteMapUpdate mvRegisterUpdate = antidoteClient.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("testCounter", counterUpdate);
		antidoteClient.updateAWMap("testAWMap", bucket, counterKey, counterUpdate);
		antidoteClient.updateAWMap("testAWMap", bucket, integerKey, intSet);
		antidoteClient.updateAWMap("testAWMap", bucket, integerKey, intInc);
		antidoteClient.updateAWMap("testAWMap", bucket, orSetKey, orSetAdd);
		antidoteClient.updateAWMap("testAWMap", bucket, orSetKey, orSetAdd2);
		antidoteClient.updateAWMap("testAWMap", bucket, orSetKey, orSetAdd3);
		antidoteClient.updateAWMap("testAWMap", bucket, orSetKey, orSetRemove);
		antidoteClient.updateAWMap("testAWMap", bucket, orSetKey, orSetRemove2);
		antidoteClient.updateAWMap("testAWMap", bucket, rwSetKey, rwSetAdd);
		antidoteClient.updateAWMap("testAWMap", bucket, rwSetKey, rwSetAdd2);
		antidoteClient.updateAWMap("testAWMap", bucket, rwSetKey, rwSetAdd3);
		antidoteClient.updateAWMap("testAWMap", bucket, rwSetKey, rwSetRemove);
		antidoteClient.updateAWMap("testAWMap", bucket, rwSetKey, rwSetRemove2);
		antidoteClient.updateAWMap("testAWMap", bucket, registerKey, registerUpdate);
		antidoteClient.updateAWMap("testAWMap", bucket, mvRegisterKey, mvRegisterUpdate);
		antidoteClient.updateAWMap("testAWMap", bucket, awMapKey, awMapUpdate);
		antidoteClient.updateAWMap("testAWMap", bucket, gMapKey, gMapUpdate);
		antidoteClient.updateGMap("testGMap", bucket, counterKey, counterUpdate);
		
		AntidoteCounter counter = antidoteClient.readCounter("testCounter", bucket);
		AntidoteORSet orSet = antidoteClient.readORSet("testORSet", bucket);
		AntidoteRWSet rwSet = antidoteClient.readRWSet("testRWSet", bucket);
		AntidoteInteger integer = antidoteClient.readInteger("testInteger", bucket);
		AntidoteRegister register = antidoteClient.readRegister("testRegister", bucket);
		AntidoteMVRegister mvRegister = antidoteClient.readMVRegister("testMVRegister", bucket);
		AntidoteAWMap awMap = antidoteClient.readAWMap("testAWMap", bucket);
		AntidoteGMap gMap = antidoteClient.readGMap("testGMap", bucket);
		
		assert(counter.getValue() == 2);
		assert(integer.getValue() == 8);
		assert(register.getValue().equals("Hi"));
		assert(mvRegister.getValueList().contains("Hi"));
		assert(orSet.getValueList().contains("Hi2"));
		assert(! orSet.getValueList().contains("Hi"));
		assert(! orSet.getValueList().contains("Hi3"));
		assert(rwSet.getValueList().contains("Hi2"));
		assert(! rwSet.getValueList().contains("Hi"));
		assert(! rwSet.getValueList().contains("Hi3"));
		assert(awMap.getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getIntegerEntry("testInteger").getValue() == 5);
		assert(awMap.getORSetEntry("testORSet").getValueList().contains("Hi3"));
		assert(awMap.getRWSetEntry("testRWSet").getValueList().contains("Hi3"));
		assert(awMap.getRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
		
		antidoteClient.removeAWMapEntry("testAWMap", bucket, counterKey);
		antidoteClient.updateRegisterBS("testRegister", bucket, ByteString.copyFromUtf8("Hi2"));
		antidoteClient.updateMVRegisterBS("testMVRegister", bucket, ByteString.copyFromUtf8("Hi2"));
		
		register.readDatabase();
		mvRegister.readDatabase();
		awMap.readDatabase();
		
		assert(register.getValue().equals("Hi2"));
		assert(mvRegister.getValueList().contains("Hi2"));
		assert(awMap.getCounterEntry("testCounter") == null);
	}
	
	@Test(timeout=500)
	public void transactionTest() {
		
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.RegisterType, "testRegister");
		AntidoteMapKey mvRegisterKey = new AntidoteMapKey(AntidoteType.MVRegisterType, "testMVRegister");

		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement();
		AntidoteMapUpdate intSet = antidoteClient.createIntegerSet(3);
		AntidoteMapUpdate intInc = antidoteClient.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetAdd = antidoteClient.createRWSetAdd("Hi");
		AntidoteMapUpdate rwSetAdd2 = antidoteClient.createRWSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate rwSetAdd3 = antidoteClient.createRWSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate orSetAdd = antidoteClient.createORSetAdd("Hi");
		AntidoteMapUpdate orSetAdd2 = antidoteClient.createORSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetAdd3 = antidoteClient.createORSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate rwSetRemove = antidoteClient.createRWSetRemove("Hi");
		AntidoteMapUpdate rwSetRemove2 = antidoteClient.createRWSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetRemove = antidoteClient.createORSetRemove("Hi");
		AntidoteMapUpdate orSetRemove2 = antidoteClient.createORSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet(ByteString.copyFromUtf8("Hi"));
		AntidoteMapUpdate mvRegisterUpdate = antidoteClient.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("testCounter", counterUpdate);
		
		ByteString descriptor = antidoteTransaction.startTransaction();
		antidoteTransaction.updateCounterTransaction("testCounter", bucket, 2, descriptor);
		antidoteTransaction.addORSetElementTransaction("testORSet", bucket, "Hi", descriptor);
		antidoteTransaction.addRWSetElementTransaction("testRWSet", bucket, "Hi", descriptor);
		antidoteTransaction.addORSetElementBSTransaction("testORSet", bucket, ByteString.copyFromUtf8("Hi2"), descriptor);
		antidoteTransaction.addRWSetElementBSTransaction("testRWSet", bucket, ByteString.copyFromUtf8("Hi2"), descriptor);
		antidoteTransaction.addORSetElementTransaction("testORSet", bucket, "Hi3", descriptor);
		antidoteTransaction.addRWSetElementTransaction("testRWSet", bucket, "Hi3", descriptor);
		antidoteTransaction.removeORSetElementBSTransaction("testORSet", bucket, ByteString.copyFromUtf8("Hi"), descriptor);
		antidoteTransaction.removeRWSetElementBSTransaction("testRWSet", bucket, ByteString.copyFromUtf8("Hi"), descriptor);
		antidoteTransaction.removeORSetElementTransaction("testORSet", bucket, "Hi3", descriptor);
		antidoteTransaction.removeRWSetElementTransaction("testRWSet", bucket, "Hi3", descriptor);
		antidoteTransaction.setIntegerTransaction("testInteger", bucket, 7, descriptor);
		antidoteTransaction.incrementIntegerTransaction("testInteger", bucket, 1, descriptor);
		antidoteTransaction.updateRegisterTransaction("testRegister", bucket, ByteString.copyFromUtf8("Hi"), descriptor);
		antidoteTransaction.updateMVRegisterTransaction("testMVRegister", bucket, "Hi", descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, counterKey, counterUpdate, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, integerKey, intSet, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, integerKey, intInc, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, orSetKey, orSetAdd, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, orSetKey, orSetAdd2, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, orSetKey, orSetAdd3, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, orSetKey, orSetRemove, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, orSetKey, orSetRemove2, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, rwSetKey, rwSetAdd, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, rwSetKey, rwSetAdd2, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, rwSetKey, rwSetAdd3, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, rwSetKey, rwSetRemove, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, rwSetKey, rwSetRemove2, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, registerKey, registerUpdate, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, mvRegisterKey, mvRegisterUpdate, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, awMapKey, awMapUpdate, descriptor);
		antidoteTransaction.updateAWMapTransaction("testAWMap", bucket, gMapKey, gMapUpdate, descriptor);
		antidoteTransaction.updateGMapTransaction("testGMap", bucket, counterKey, counterUpdate, descriptor);
		
		AntidoteCounter counter = antidoteTransaction.readCounterTransaction("testCounter", bucket, descriptor);
		AntidoteORSet orSet = antidoteTransaction.readORSetTransaction("testORSet", bucket, descriptor);
		AntidoteRWSet rwSet = antidoteTransaction.readRWSetTransaction("testRWSet", bucket, descriptor);
		AntidoteInteger integer = antidoteTransaction.readIntegerTransaction("testInteger", bucket, descriptor);
		AntidoteRegister register = antidoteTransaction.readRegisterTransaction("testRegister", bucket, descriptor);
		AntidoteMVRegister mvRegister = antidoteTransaction.readMVRegisterTransaction("testMVRegister", bucket, descriptor);
		AntidoteAWMap awMap = antidoteTransaction.readAWMapTransaction("testAWMap", bucket, descriptor);
		AntidoteGMap gMap = antidoteTransaction.readGMapTransaction("testGMap", bucket, descriptor);
		
		antidoteTransaction.commitTransaction(descriptor);
		
		assert(counter.getValue() == 2);
		assert(integer.getValue() == 8);
		assert(register.getValue().equals("Hi"));
		assert(mvRegister.getValueList().contains("Hi"));
		assert(orSet.getValueList().contains("Hi2"));
		assert(! orSet.getValueList().contains("Hi"));
		assert(! orSet.getValueList().contains("Hi3"));
		assert(rwSet.getValueList().contains("Hi2"));
		assert(! rwSet.getValueList().contains("Hi"));
		assert(! rwSet.getValueList().contains("Hi3"));
		assert(awMap.getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getIntegerEntry("testInteger").getValue() == 5);
		assert(awMap.getORSetEntry("testORSet").getValueList().contains("Hi3"));
		assert(awMap.getRWSetEntry("testRWSet").getValueList().contains("Hi3"));
		assert(awMap.getRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
	}
	
	@Test(timeout=2000)
	public void counterTest2() {
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement(5);
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapCounterEntry counter1 = testAWMap.getCounterEntry("counterKey");
		AntidoteMapCounterEntry counter2 = testAWMap2.getCounterEntry("counterKey");
		AntidoteMapCounterEntry counter3 = testAWMap3.getCounterEntry("counterKey");

		counter1.increment(1);
		counter2.increment(2);
		counter3.increment(3);
		counter1.synchronize();
		counter2.synchronize();
		counter3.synchronize();
		
		assert(counter1.getValue()==6);
		assert(counter2.getValue()==7);
		assert(counter3.getValue()==8);
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		counter1 = testGMap.getCounterEntry("counterKey");
		counter2 = testGMap2.getCounterEntry("counterKey");
		counter3 = testGMap3.getCounterEntry("counterKey");

		counter1.increment(1);
		counter2.increment(2);
		counter3.increment(3);
		counter1.synchronize();
		counter2.synchronize();
		counter3.synchronize();
		assert(counter1.getValue()==6);
		assert(counter2.getValue()==7);
		assert(counter3.getValue()==8);
	}
	
	@Test(timeout=2000)
	public void integerTest2() {
		AntidoteMapUpdate integerUpdate = antidoteClient.createIntegerIncrement(5);
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapIntegerEntry integer1 = testAWMap.getIntegerEntry("integerKey");
		AntidoteMapIntegerEntry integer2 = testAWMap2.getIntegerEntry("integerKey");
		AntidoteMapIntegerEntry integer3 = testAWMap3.getIntegerEntry("integerKey");

		integer1.increment(1);
		integer2.increment(2);
		integer3.increment(3);
		integer1.synchronize();
		integer2.synchronize();
		integer3.synchronize();
		
		assert(integer1.getValue()==6);
		assert(integer2.getValue()==7);
		assert(integer3.getValue()==8);
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		integer1 = testGMap.getIntegerEntry("integerKey");
		integer2 = testGMap2.getIntegerEntry("integerKey");
		integer3 = testGMap3.getIntegerEntry("integerKey");

		integer1.increment(1);
		integer2.increment(2);
		integer3.increment(3);

		integer1.synchronize();
		integer2.synchronize();
		integer3.synchronize();
		assert(integer1.getValue()==6);
		assert(integer2.getValue()==7);
		assert(integer3.getValue()==8);
	}
	
	@Test(timeout=2000)
	public void orSetTest2() {
		AntidoteMapUpdate orSetUpdate = antidoteClient.createORSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapORSetEntry orSet1 = testAWMap.getORSetEntry("orSetKey");
		AntidoteMapORSetEntry orSet2 = testAWMap2.getORSetEntry("orSetKey");
		AntidoteMapORSetEntry orSet3 = testAWMap3.getORSetEntry("orSetKey");

		orSet1.addElement("Hi2");
		orSet2.addElement("Hi3");
		orSet3.addElement("Hi4");
		orSet1.synchronize();
		orSet2.synchronize();
		orSet3.synchronize();
		
		assert(orSet1.getValueList().contains("Hi2"));
		assert(orSet2.getValueList().contains("Hi3"));
		assert(orSet3.getValueList().contains("Hi4"));
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		orSet1 = testGMap.getORSetEntry("orSetKey");
		orSet2 = testGMap2.getORSetEntry("orSetKey");
		orSet3 = testGMap3.getORSetEntry("orSetKey");

		orSet1.addElement("Hi2");
		orSet2.addElement("Hi3");
		orSet3.addElement("Hi4");
		orSet1.synchronize();
		orSet2.synchronize();
		orSet3.synchronize();
		
		assert(orSet1.getValueList().contains("Hi2"));
		assert(orSet2.getValueList().contains("Hi3"));
		assert(orSet3.getValueList().contains("Hi4"));
	}
	
	@Test(timeout=2000)
	public void rwSetTest2() {
		AntidoteMapUpdate rwSetUpdate = antidoteClient.createRWSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapRWSetEntry rwSet1 = testAWMap.getRWSetEntry("rwSetKey");
		AntidoteMapRWSetEntry rwSet2 = testAWMap2.getRWSetEntry("rwSetKey");
		AntidoteMapRWSetEntry rwSet3 = testAWMap3.getRWSetEntry("rwSetKey");

		rwSet1.addElement("Hi2");
		rwSet2.addElement("Hi3");
		rwSet3.addElement("Hi4");
		rwSet1.synchronize();
		rwSet2.synchronize();
		rwSet3.synchronize();
		
		assert(rwSet1.getValueList().contains("Hi2"));
		assert(rwSet2.getValueList().contains("Hi3"));
		assert(rwSet3.getValueList().contains("Hi4"));
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		rwSet1 = testGMap.getRWSetEntry("rwSetKey");
		rwSet2 = testGMap2.getRWSetEntry("rwSetKey");
		rwSet3 = testGMap3.getRWSetEntry("rwSetKey");

		rwSet1.addElement("Hi2");
		rwSet2.addElement("Hi3");
		rwSet3.addElement("Hi4");
		rwSet1.synchronize();
		rwSet2.synchronize();
		rwSet3.synchronize();
		
		assert(rwSet1.getValueList().contains("Hi2"));
		assert(rwSet2.getValueList().contains("Hi3"));
		assert(rwSet3.getValueList().contains("Hi4"));
	}
	
	@Test(timeout=2000)
	public void registerTest2() {
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.RegisterType, "registerKey"), registerUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapRegisterEntry register1 = testAWMap.getRegisterEntry("registerKey");
		AntidoteMapRegisterEntry register2 = testAWMap2.getRegisterEntry("registerKey");
		AntidoteMapRegisterEntry register3 = testAWMap3.getRegisterEntry("registerKey");

		register1.setValueBS(ByteString.copyFromUtf8("Hi2"));
		register2.setValueBS(ByteString.copyFromUtf8("Hi3"));
		register3.setValueBS(ByteString.copyFromUtf8("Hi4"));
		register1.synchronize();
		register2.synchronize();
		register3.synchronize();
		
		assert(register1.getValue().equals("Hi2"));
		assert(register2.getValue().equals("Hi3"));
		assert(register3.getValue().equals("Hi4"));
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.RegisterType, "registerKey"), registerUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		register1 = testGMap.getRegisterEntry("registerKey");
		register2 = testGMap2.getRegisterEntry("registerKey");
		register3 = testGMap3.getRegisterEntry("registerKey");

		register1.setValue("Hi2");
		register2.setValue("Hi3");
		register3.setValue("Hi4");

		register1.synchronize();
		register2.synchronize();
		register3.synchronize();
		assert(register1.getValue().equals("Hi2"));
		assert(register2.getValue().equals("Hi3"));
		assert(register3.getValue().equals("Hi4"));
	}
	
	@Test(timeout=2000)
	public void mvRegisterTest2() {
		AntidoteMapUpdate mvRegisterUpdate = antidoteClient.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		antidoteClient.updateAWMap("outerAWMap", bucket, new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteAWMap testAWMap = antidoteClient.readAWMap("outerAWMap", bucket);
		AntidoteMapAWMapEntry testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteMapAWMapEntry testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteMapMVRegisterEntry mvRegister1 = testAWMap.getMVRegisterEntry("mvRegisterKey");
		AntidoteMapMVRegisterEntry mvRegister2 = testAWMap2.getMVRegisterEntry("mvRegisterKey");
		AntidoteMapMVRegisterEntry mvRegister3 = testAWMap3.getMVRegisterEntry("mvRegisterKey");

		mvRegister1.setValueBS(ByteString.copyFromUtf8("Hi2"));
		mvRegister2.setValueBS(ByteString.copyFromUtf8("Hi3"));
		mvRegister3.setValueBS(ByteString.copyFromUtf8("Hi4"));
		mvRegister1.synchronize();
		mvRegister2.synchronize();
		mvRegister3.synchronize();
		
		assert(mvRegister1.getValueList().contains("Hi2"));
		assert(mvRegister2.getValueList().contains("Hi3"));
		assert(mvRegister3.getValueList().contains("Hi4"));
		
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerGMap", gMapUpdate);
		
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		antidoteClient.updateGMap("outerGMap", bucket, new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		
		AntidoteGMap testGMap = antidoteClient.readGMap("outerGMap", bucket);
		AntidoteMapGMapEntry testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteMapGMapEntry testGMap3 = testGMap2.getGMapEntry("innerGMap");

		mvRegister1 = testGMap.getMVRegisterEntry("mvRegisterKey");
		mvRegister2 = testGMap2.getMVRegisterEntry("mvRegisterKey");
		mvRegister3 = testGMap3.getMVRegisterEntry("mvRegisterKey");

		mvRegister1.setValue("Hi2");
		mvRegister2.setValue("Hi3");
		mvRegister3.setValue("Hi4");

		mvRegister1.synchronize();
		mvRegister2.synchronize();
		mvRegister3.synchronize();
		assert(mvRegister1.getValueList().contains("Hi2"));
		assert(mvRegister2.getValueList().contains("Hi3"));
		assert(mvRegister3.getValueList().contains("Hi4"));
	}
}
