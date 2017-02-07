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
		LowLevelCounter lowCounter1 = new LowLevelCounter("testCounter5", bucket, antidoteClient);
		LowLevelCounter lowCounter2 = new LowLevelCounter("testCounter3", bucket, antidoteClient);
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter();
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter();
		int oldValue2 = counter2old.getValue();
		antidoteTransaction.startTransaction();
		lowCounter1.increment(5, antidoteTransaction);
		lowCounter2.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter();
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter();
		int newValue1 = counter1new.getValue();
		assert (newValue1 == oldValue1+5);
		int newValue2 = counter2new.getValue();
		assert (newValue2 == oldValue2+3);
	}	
	
	@Test(timeout=1000) 
	public void incBy2Test() {		
		LowLevelCounter lowCounter = new LowLevelCounter("testCounter5", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter();
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
	
	@Test(timeout=2000)
	public void decrementToZeroTest() {
		LowLevelCounter lowCounter = new LowLevelCounter("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter testCounter = lowCounter.createAntidoteCounter();
		testCounter.increment(0-testCounter.getValue());
		testCounter.push();
		assert(testCounter.getValue() == 0); //operation executed locally
		testCounter.readDatabase();
		assert(testCounter.getValue() == 0); //operation executed in the data base
	}
	
	@Test(timeout=2000)
	public void incBy5Test(){	
		LowLevelCounter lowCounter = new LowLevelCounter("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter();
		int oldValue = counter.getValue();
		counter.increment(5);
		counter.push();
		int newValue = counter.getValue();
		assert(newValue == oldValue+5);
		counter.readDatabase();
		newValue = counter.getValue();
		assert(newValue == oldValue+5);		
	}
	
	@Test(timeout=2000)
	public void addElemTest() {
		LowLevelORSet lowSet = new LowLevelORSet("testSet", bucket, antidoteClient);
		AntidoteOuterORSet testSet = lowSet.createAntidoteORSet();
		testSet.addElement("element");
		testSet.push();
		assert(testSet.getValueList().contains("element"));
		testSet.readDatabase();
		assert(testSet.getValueList().contains("element"));
	}
	
	@Test(timeout=2000)
	public void remElemTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		LowLevelORSet lowSet = new LowLevelORSet("testSet", bucket, antidoteClient);
		AntidoteOuterORSet testSet = lowSet.createAntidoteORSet();
		testSet.addElement(elements);
		testSet.removeElement("Hi");
		testSet.push();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
		testSet.readDatabase();
		assert(! testSet.getValueList().contains("Hi"));
		assert(testSet.getValueList().contains("Bye"));
	}
	
	@Test(timeout=2000)
	public void addElemsTest() {
		LowLevelRWSet lowSet = new LowLevelRWSet("testSet1", bucket, antidoteClient);
		List<String> elements = new ArrayList<String>();
		elements.add("Wall");
		elements.add("Ball");
		AntidoteOuterRWSet testSet = lowSet.createAntidoteRWSet();
		testSet.addElement(elements);
		testSet.push();
		testSet.readDatabase();
		assert(testSet.getValueList().contains("Wall"));
		assert(testSet.getValueList().contains("Ball"));
	}
	
	@Test(timeout=2000)
	public void remElemsTest() {
		LowLevelRWSet lowSet = new LowLevelRWSet("testSet1", bucket, antidoteClient);
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteOuterRWSet testSet = lowSet.createAntidoteRWSet();
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
	
	@Test(timeout=2000)
	public void updateRegTest() {
		LowLevelLWWRegister lowReg = new LowLevelLWWRegister("testReg", bucket, antidoteClient);
        AntidoteOuterLWWRegister testReg = lowReg.createAntidoteLWWRegister();
        testReg.setValue("hi");
        testReg.setValue("bye");
        testReg.push();
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
	}
	
	@Test(timeout=2000)
	public void updateMVRegTest() {
		LowLevelMVRegister lowReg = new LowLevelMVRegister("testReg", bucket, antidoteClient);

		AntidoteOuterMVRegister testReg = lowReg.createAntidoteMVRegister();
        testReg.setValue("hi");
        testReg.setValue("bye");
        testReg.push();
        assert(testReg.getValueList().contains("bye"));
        assert(! testReg.getValueList().contains("hi"));
	}
	
	@Test(timeout=2000)
	public void incIntBy1Test() {
		LowLevelInteger lowInt = new LowLevelInteger("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger();
		int oldValue = integer.getValue();
		integer.increment();
		integer.push();
		int newValue = integer.getValue();
		assert(oldValue+1 == newValue);
		integer.readDatabase();
		newValue = integer.getValue();
		assert(oldValue+1 == newValue);
	}
	
	@Test(timeout=2000)
	public void decBy5Test() {
		LowLevelInteger lowInt = new LowLevelInteger("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger();
		int oldValue = integer.getValue();
		integer.increment(-5);
		integer.push();
		int newValue = integer.getValue();
		assert(oldValue-5 == newValue);
		integer.readDatabase();
		newValue = integer.getValue();
		assert(oldValue-5 == newValue);
	}
	
	@Test(timeout=2000)
	public void setIntTest() {					
		LowLevelInteger lowInt = new LowLevelInteger("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger();
		integer.setValue(42);
		integer.push();
		assert(integer.getValue() == 42);	
		integer.readDatabase();
		assert(integer.getValue() == 42);	
	}

	@Test(timeout=1000)
	public void counterTest() {
		String counterKey = "counterKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap12", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement(5);
		testMap.update(counterKey, counterUpdate);
		int counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert (counterValue == 5); //local value is 5
		testMap.synchronize();
		AntidoteInnerCounter counter = testMap.getCounterEntry(counterKey);
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
		testMap.remove(counterKey, AntidoteType.CounterType);
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=2000)
	public void integerTest() {
		String integerKey = "integerKey";
		String mapKey = "mapKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap12", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate integerUpdate = antidoteClient.createIntegerIncrement(5);
		AntidoteMapUpdate mapUpdate = antidoteClient.createAWMapUpdate(integerKey, integerUpdate);
		testMap.update(mapKey, mapUpdate);
		AntidoteInnerAWMap innerMap = testMap.getAWMapEntry(mapKey);
		int integerValue = innerMap.getIntegerEntry(integerKey).getValue();
		assert (integerValue == 5); //local value is 5
		testMap.synchronize();
		innerMap = testMap.getAWMapEntry(mapKey); //overwrite local content with database content
		AntidoteInnerInteger integer = innerMap.getIntegerEntry(integerKey);
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
		testMap.remove(mapKey, AntidoteType.AWMapType);
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=1000)
	public void registerTest() {
		String registerKey = "registerKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate registerUpdate = antidoteClient.createRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		String registerValue = testMap.getLWWRegisterEntry(registerKey).getValue();
		assert (registerValue.equals("yes")); //local value is "yes"
		AntidoteInnerLWWRegister register = testMap.getLWWRegisterEntry(registerKey);
		testMap.synchronize();
		registerValue = testMap.getLWWRegisterEntry(registerKey).getValue();
		assert(registerValue.equals("yes")); //update forwarded to database, then got a new state from database
		register = testMap.getLWWRegisterEntry(registerKey);
		register.setValue("no");
		register.setValue("maybe");
		assert(register.getValue().equals("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValue().equals("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.remove(registerKey, AntidoteType.LWWRegisterType); 
		testMap.push(); // everything set to initial situation
	}

	@Test(timeout=2000)
	public void mvRegisterTest() {
		String registerKey = "mvRegisterKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate registerUpdate = antidoteClient.createMVRegisterSet("yes");
		testMap.update(registerKey, registerUpdate);
		List<String> registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert (registerValueList.contains("yes")); //local value is "yes"
		AntidoteInnerMVRegister register = testMap.getMVRegisterEntry(registerKey);
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
		testMap.remove(registerKey, AntidoteType.MVRegisterType); 
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=2000)
	public void orSetTest() {
		String setKey = "orSetKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate setUpdate = antidoteClient.createORSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getORSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteInnerORSet set = testMap.getORSetEntry(setKey);
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
		testMap.remove(setKey, AntidoteType.ORSetType); 
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=2000)
	public void rwSetTest() {
		String setKey = "rwSetKey";
		LowLevelAWMap lowMap = new LowLevelAWMap("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		AntidoteMapUpdate setUpdate = antidoteClient.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
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
		testMap.remove(setKey, AntidoteType.RWSetType);
		testMap.push(); // everything set to initial situation
	}
	
	@Test(timeout=2000)
	public void rwSetTest4() {
		String setKey = "rwSetKey";
		LowLevelGMap lowMap = new LowLevelGMap("testMapBestMap4", bucket, antidoteClient);
		AntidoteOuterGMap testMap = lowMap.createAntidoteGMap();
		AntidoteMapUpdate setUpdate = antidoteClient.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate);
		List <String> setValueList = testMap.getRWSetEntry(setKey).getValueList();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
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
	
	@Test(timeout=10000)
	public void createRemoveTest() {
		LowLevelAWMap lowMap = new LowLevelAWMap("emptyMapBestMap", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();
		
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

		AntidoteMapUpdate orSetRemove = antidoteClient.createMapRemove(orSetKey, AntidoteType.ORSetType);
		AntidoteMapUpdate rwSetRemove = antidoteClient.createMapRemove(rwSetKey, AntidoteType.RWSetType);
		AntidoteMapUpdate counterRemove = antidoteClient.createMapRemove(counterKey, AntidoteType.CounterType);
		AntidoteMapUpdate integerRemove = antidoteClient.createMapRemove(integerKey, AntidoteType.IntegerType);
		AntidoteMapUpdate registerRemove = antidoteClient.createMapRemove(registerKey, AntidoteType.LWWRegisterType);
		AntidoteMapUpdate mvRegisterRemove = antidoteClient.createMapRemove(mvRegisterKey, AntidoteType.MVRegisterType);
		AntidoteMapUpdate awMapRemove = antidoteClient.createMapRemove(innerAWMapKey, AntidoteType.AWMapType);
		AntidoteMapUpdate gMapRemove = antidoteClient.createMapRemove(gMapKey, AntidoteType.GMapType);

		testMap.update(awMapKey, orSetRemove);
		testMap.update(awMapKey, rwSetRemove);
		testMap.update(awMapKey, counterRemove);
		testMap.update(awMapKey, integerRemove);
		testMap.update(awMapKey, registerRemove);
		testMap.update(awMapKey, mvRegisterRemove);
		testMap.update(awMapKey, awMapRemove);
		testMap.update(awMapKey, gMapRemove);
		
		testMap.synchronize();

		AntidoteInnerAWMap innerMap = testMap.getAWMapEntry(awMapKey);
		
		assert(innerMap.getEntryList().size()==0);
	}
	
	@Test(timeout=10000)
	public void updateTest() {
		LowLevelCounter lowCounter = new LowLevelCounter("testCounter", bucket, antidoteClient);
		LowLevelInteger lowInteger = new LowLevelInteger("testInteger", bucket, antidoteClient);
		LowLevelLWWRegister lowLWWRegister = new LowLevelLWWRegister("testRegister", bucket, antidoteClient);
		LowLevelMVRegister lowMVRegister = new LowLevelMVRegister("testMVRegister", bucket, antidoteClient);
		LowLevelORSet lowORSet = new LowLevelORSet("testORSet", bucket, antidoteClient);
		LowLevelRWSet lowRWSet = new LowLevelRWSet("testRWSet", bucket, antidoteClient);
		LowLevelAWMap lowAWMap = new LowLevelAWMap("testAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("testGMap", bucket, antidoteClient);

		lowCounter.increment(2);
		lowInteger.set(7);
		lowInteger.increment(1);
		lowLWWRegister.set("Hi");
		lowMVRegister.set("Hi");

		lowORSet.add("Hi");
		lowRWSet.add("Hi");
		lowORSet.addBS(ByteString.copyFromUtf8("Hi2"));
		lowRWSet.addBS(ByteString.copyFromUtf8("Hi2"));
		lowORSet.add("Hi3");
		lowRWSet.add("Hi3");
		lowORSet.removeBS(ByteString.copyFromUtf8("Hi"));
		lowRWSet.removeBS(ByteString.copyFromUtf8("Hi"));
		lowORSet.remove("Hi3");
		lowRWSet.remove("Hi3");
		
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.LWWRegisterType, "testRegister");
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
		lowAWMap.update(counterKey, counterUpdate);
		lowAWMap.update(integerKey, intSet);
		lowAWMap.update(integerKey, intInc);
		lowAWMap.update(orSetKey, orSetAdd);
		lowAWMap.update(orSetKey, orSetAdd2);
		lowAWMap.update(orSetKey, orSetAdd3);
		lowAWMap.update(orSetKey, orSetRemove);
		lowAWMap.update(orSetKey, orSetRemove2);
		lowAWMap.update(rwSetKey, rwSetAdd);
		lowAWMap.update(rwSetKey, rwSetAdd2);
		lowAWMap.update(rwSetKey, rwSetAdd3);
		lowAWMap.update(rwSetKey, rwSetRemove);
		lowAWMap.update(rwSetKey, rwSetRemove2);
		lowAWMap.update(registerKey, registerUpdate);
		lowAWMap.update(mvRegisterKey, mvRegisterUpdate);
		lowAWMap.update(awMapKey, awMapUpdate);
		lowAWMap.update(gMapKey, gMapUpdate);
		lowGMap.update(counterKey, counterUpdate);
		
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter();
		AntidoteOuterORSet orSet = lowORSet.createAntidoteORSet();
		AntidoteOuterRWSet rwSet = lowRWSet.createAntidoteRWSet();
		AntidoteOuterInteger integer = lowInteger.createAntidoteInteger();
		AntidoteOuterLWWRegister register = lowLWWRegister.createAntidoteLWWRegister();
		AntidoteOuterMVRegister mvRegister = lowMVRegister.createAntidoteMVRegister();
		AntidoteOuterAWMap awMap = lowAWMap.createAntidoteAWMap();
		AntidoteOuterGMap gMap = lowGMap.createAntidoteGMap();

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
		assert(awMap.getLWWRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
		
		lowAWMap.remove(counterKey);
		lowLWWRegister.setBS(ByteString.copyFromUtf8("Hi2"));
		lowMVRegister.setBS(ByteString.copyFromUtf8("Hi2"));
		
		register.readDatabase();
		mvRegister.readDatabase();
		awMap.readDatabase();
		
		assert(register.getValue().equals("Hi2"));
		assert(mvRegister.getValueList().contains("Hi2"));
		assert(awMap.getCounterEntry("testCounter") == null);
	}
	
	@Test(timeout=10000)
	public void transactionTest() {
		LowLevelLWWRegister lowLWWRegister = new LowLevelLWWRegister("testRegister", bucket, antidoteClient);
		LowLevelMVRegister lowMVRegister = new LowLevelMVRegister("testMVRegister", bucket, antidoteClient);
		LowLevelORSet lowORSet = new LowLevelORSet("testORSet", bucket, antidoteClient);
		LowLevelRWSet lowRWSet = new LowLevelRWSet("testRWSet", bucket, antidoteClient);
		LowLevelAWMap lowAWMap = new LowLevelAWMap("testAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("testGMap", bucket, antidoteClient);
		
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.LWWRegisterType, "testRegister");
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
		antidoteTransaction.startTransaction();
		LowLevelCounter lowCounter = new LowLevelCounter("testCounter", bucket, antidoteClient);
		LowLevelInteger lowInteger = new LowLevelInteger("testInteger", bucket, antidoteClient);
		lowCounter.increment(2, antidoteTransaction);
		lowInteger.set(7, antidoteTransaction);
		lowInteger.increment(1, antidoteTransaction);
		
		lowORSet.add("Hi", antidoteTransaction);
		lowRWSet.add("Hi", antidoteTransaction);
		lowORSet.addBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		lowRWSet.addBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		lowORSet.add("Hi3", antidoteTransaction);
		lowRWSet.add("Hi3", antidoteTransaction);
		lowORSet.removeBS(ByteString.copyFromUtf8("Hi"), antidoteTransaction);
		lowRWSet.removeBS(ByteString.copyFromUtf8("Hi"), antidoteTransaction);
		lowORSet.remove("Hi3", antidoteTransaction);
		lowRWSet.remove("Hi3", antidoteTransaction);
		lowLWWRegister.setBS(ByteString.copyFromUtf8("Hi"), antidoteTransaction);
		lowMVRegister.set("Hi", antidoteTransaction);
		lowAWMap.update(counterKey, counterUpdate, antidoteTransaction);
		lowAWMap.update(integerKey, intSet, antidoteTransaction);
		lowAWMap.update(integerKey, intInc, antidoteTransaction);
		lowAWMap.update(orSetKey, orSetAdd, antidoteTransaction);
		lowAWMap.update(orSetKey, orSetAdd2, antidoteTransaction);
		lowAWMap.update(orSetKey, orSetAdd3, antidoteTransaction);
		lowAWMap.update(orSetKey, orSetRemove, antidoteTransaction);
		lowAWMap.update(orSetKey, orSetRemove2, antidoteTransaction);
		lowAWMap.update(rwSetKey, rwSetAdd, antidoteTransaction);
		lowAWMap.update(rwSetKey, rwSetAdd2, antidoteTransaction);
		lowAWMap.update(rwSetKey, rwSetAdd3, antidoteTransaction);
		lowAWMap.update(rwSetKey, rwSetRemove, antidoteTransaction);
		lowAWMap.update(rwSetKey, rwSetRemove2, antidoteTransaction);
		lowAWMap.update(registerKey, registerUpdate, antidoteTransaction);
		lowAWMap.update(mvRegisterKey, mvRegisterUpdate, antidoteTransaction);
		lowAWMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		lowAWMap.update(gMapKey, gMapUpdate, antidoteTransaction);
		lowGMap.update(counterKey, counterUpdate, antidoteTransaction);
		
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
		AntidoteOuterORSet orSet = lowORSet.createAntidoteORSet(antidoteTransaction);
		AntidoteOuterRWSet rwSet = lowRWSet.createAntidoteRWSet(antidoteTransaction);
		AntidoteOuterInteger integer = lowInteger.createAntidoteInteger(antidoteTransaction);
		AntidoteOuterLWWRegister register = lowLWWRegister.createAntidoteLWWRegister(antidoteTransaction);
		AntidoteOuterMVRegister mvRegister = lowMVRegister.createAntidoteMVRegister(antidoteTransaction);
		AntidoteOuterAWMap awMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteOuterGMap gMap = lowGMap.createAntidoteGMap(antidoteTransaction);
		
		antidoteTransaction.commitTransaction();
		
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
		assert(awMap.getLWWRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
	}
	
	@Test(timeout=2000)
	public void counterTest2() {
		AntidoteMapUpdate counterUpdate = antidoteClient.createCounterIncrement(5);
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate gMapUpdate = antidoteClient.createGMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerGMap", gMapUpdate);
		AntidoteMapUpdate gMapUpdate2 = antidoteClient.createGMapUpdate("innerAWMap", awMapUpdate);
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerGMap testGMap3 = testAWMap2.getGMapEntry("innerGMap");

		AntidoteInnerCounter counter1 = testAWMap.getCounterEntry("counterKey");
		AntidoteInnerCounter counter2 = testAWMap2.getCounterEntry("counterKey");
		AntidoteInnerCounter counter3 = testGMap3.getCounterEntry("counterKey");

		counter1.increment(1);
		counter2.increment(2);
		counter3.increment(3);
		counter1.synchronize();
		counter2.synchronize();
		counter3.synchronize();
		
		assert(counter1.getValue()==6);
		assert(counter2.getValue()==7);
		assert(counter3.getValue()==8);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerAWMap testAWMap3 = testGMap2.getAWMapEntry("innerAWMap");

		counter1 = testGMap.getCounterEntry("counterKey");
		counter2 = testGMap2.getCounterEntry("counterKey");
		counter3 = testAWMap3.getCounterEntry("counterKey");

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
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteInnerInteger integer1 = testAWMap.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer2 = testAWMap2.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer3 = testAWMap3.getIntegerEntry("integerKey");

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
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");

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
	
	@Test(timeout=3000)
	public void orSetTest2() {
		AntidoteMapUpdate orSetUpdate = antidoteClient.createORSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = antidoteClient.createAWMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = antidoteClient.createAWMapUpdate("innerAWMap", awMapUpdate);
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteInnerORSet orSet1 = testAWMap.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet2 = testAWMap2.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet3 = testAWMap3.getORSetEntry("orSetKey");

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
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");

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
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteInnerRWSet rwSet1 = testAWMap.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet2 = testAWMap2.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet3 = testAWMap3.getRWSetEntry("rwSetKey");

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
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");

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
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteInnerLWWRegister register1 = testAWMap.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register2 = testAWMap2.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register3 = testAWMap3.getLWWRegisterEntry("registerKey");

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
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");

		register1 = testGMap.getLWWRegisterEntry("registerKey");
		register2 = testGMap2.getLWWRegisterEntry("registerKey");
		register3 = testGMap3.getLWWRegisterEntry("registerKey");

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
		
		LowLevelAWMap lowAWMap = new LowLevelAWMap("outerAWMap", bucket, antidoteClient);
		LowLevelGMap lowGMap = new LowLevelGMap("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");

		AntidoteInnerMVRegister mvRegister1 = testAWMap.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister2 = testAWMap2.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister3 = testAWMap3.getMVRegisterEntry("mvRegisterKey");

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
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
				
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");

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
