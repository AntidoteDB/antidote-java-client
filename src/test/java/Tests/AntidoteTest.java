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
	}

	public String nextSessionId() {
		SecureRandom random = new SecureRandom();
		return new BigInteger(130, random).toString(32);
	}

	@Test(timeout=10000)
	public void readstaticTransaction() {
		List<AntidoteCRDT> antidoteCRDTObjects = new ArrayList<>();
		List<ObjectRef> objectRefs = new ArrayList<>();
		CounterRef lowCounter3 =antidoteClient.counterRef("testCounter1", bucket);
		CounterRef lowCounter4 = antidoteClient.counterRef("testCounter2", bucket);
		CounterRef lowCounter = antidoteClient.counterRef("testCounter3", bucket);
		CounterRef lowCounter1 = antidoteClient.counterRef("testCounter4", bucket);
		IntegerRef lowInt =antidoteClient.integerRef("testInteger1", bucket);
		IntegerRef lowInt2 = antidoteClient.integerRef("testInteger2", bucket);
		IntegerRef lowInt3 = antidoteClient.integerRef("testInteger3", bucket);
		IntegerRef lowInt4 = antidoteClient.integerRef("testInteger4", bucket);
		AntidoteOuterCounter counter1 = lowCounter1.createAntidoteCounter();
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter();
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger();
		AntidoteOuterInteger integer2 = lowInt2.createAntidoteInteger();


		AntidoteTransaction tx = antidoteClient.createStaticTransaction();
		lowCounter3.increment(2,tx);
		lowCounter4.increment(3,tx);
		lowInt3.increment(4,tx);
		lowInt4.increment(5,tx);
		integer.increment(6,tx);
		integer2.increment(7,tx);
		counter.increment(8,tx);
		counter1.increment(9,tx);
		tx.commitTransaction();
		tx.close();

		antidoteCRDTObjects.add(integer);
		antidoteCRDTObjects.add(integer2);
		antidoteCRDTObjects.add(counter);
		antidoteCRDTObjects.add(counter1);

		objectRefs.add(lowCounter3);
		objectRefs.add(lowCounter4);
		objectRefs.add(lowInt3);
		objectRefs.add(lowInt4);



		AntidoteStaticTransaction antidoteStaticTransaction = new AntidoteStaticTransaction(antidoteClient);

		List<AntidoteCRDT> objectRefsList = antidoteStaticTransaction.readObjects(objectRefs);
		List<AntidoteCRDT> antidoteCRDTList= antidoteStaticTransaction.readOuterObjects(antidoteCRDTObjects);

		assert (lowCounter3.getValue(objectRefsList)==2);
		assert (lowCounter4.getValue(objectRefsList)==3);
		assert (lowInt3.getValue(objectRefsList)==4);
		assert (lowInt4.getValue(objectRefsList)==5);
		assert (integer.getValue(antidoteCRDTList)==6);
		assert (integer2.getValue(antidoteCRDTList)==7);
		assert (counter.getValue(antidoteCRDTList)==8);
		assert (counter1.getValue(antidoteCRDTList)==9);
	}



	@Test(timeout=10000)
	public void counterRefCommitStaticTransaction() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter1 = new CounterRef("testCounter5", bucket, antidoteClient);
		CounterRef lowCounter2 = new CounterRef("testCounter3", bucket, antidoteClient);
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter(antidoteTransaction);
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter(antidoteTransaction);
		int oldValue2 = counter2old.getValue();
		antidoteTransaction.commitTransaction();
		antidoteTransaction.close();
		AntidoteTransaction tx = antidoteClient.createStaticTransaction();
		lowCounter1.increment(5, tx);
		lowCounter1.increment(5, tx);
		lowCounter2.increment(3, tx);
		lowCounter2.increment(3, tx);
		tx.commitTransaction();
		tx.close();
		antidoteTransaction = antidoteClient.createTransaction();
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter(antidoteTransaction);
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter(antidoteTransaction);
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter();
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter();
		int newValue1 = counter1new.getValue();
		int newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		antidoteTransaction.close();
		assert (newValue1 == oldValue1+10);
		assert (newValue2 == oldValue2+6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1new.readDatabase(antidoteTransaction);
		counter2new.readDatabase(antidoteTransaction);
		newValue1 = counter1new.getValue();
		newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		antidoteTransaction.close();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
	}
	@Test(timeout=10000)
	public void counterRefCommitTransaction() {
		CounterRef lowCounter1 = new CounterRef("testCounter5", bucket, antidoteClient);
		CounterRef lowCounter2 = new CounterRef("testCounter3", bucket, antidoteClient);
		antidoteTransaction = antidoteClient.createTransaction();
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter(antidoteTransaction);
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter(antidoteTransaction);
		int oldValue2 = counter2old.getValue();
<<<<<<< HEAD
		antidoteTransaction.commitTransaction();

=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteTransaction tx = antidoteClient.createTransaction();
		lowCounter1.increment(5, tx);
		lowCounter1.increment(5, tx);
		lowCounter2.increment(3, tx);
		lowCounter2.increment(3, tx);
		tx.commitTransaction();
		tx.close();
<<<<<<< HEAD
		
		antidoteTransaction = antidoteClient.createTransaction();
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter(antidoteTransaction);
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter(antidoteTransaction);
		antidoteTransaction.commitTransaction();
=======
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter();
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		int newValue1 = counter1new.getValue();
		int newValue2 = counter2new.getValue();
		assert (newValue1 == oldValue1+10);
		assert (newValue2 == oldValue2+6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1new.readDatabase(antidoteTransaction);
		counter2new.readDatabase(antidoteTransaction);
		newValue1 = counter1new.getValue();
		newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
	}
	@Test(timeout=10000)
	public void commitTransaction() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter1 = new CounterRef("testCounter5", bucket, antidoteClient);
		CounterRef lowCounter2 = new CounterRef("testCounter3", bucket, antidoteClient);
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter(antidoteTransaction);
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter(antidoteTransaction);
		int oldValue2 = counter2old.getValue();
<<<<<<< HEAD
		antidoteTransaction.commitTransaction();

=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteTransaction tx3 = antidoteClient.createTransaction();
		counter1old.increment(5, tx3);
		counter1old.increment(5, tx3);
		counter2old.increment(3, tx3);
		counter2old.increment(3, tx3);
		tx3.commitTransaction();
		tx3.close();
<<<<<<< HEAD

		antidoteTransaction = antidoteClient.createTransaction();
=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		int newValue1 = counter1old.getValue();
		int newValue2 = counter2old.getValue();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
		counter1old.readDatabase(antidoteTransaction);
		counter2old.readDatabase(antidoteTransaction);
		newValue1 = counter1old.getValue();
		newValue2 = counter2old.getValue();
		antidoteTransaction.commitTransaction();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
	}
<<<<<<< HEAD


=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	@Test(timeout=10000)
	public void commitStaticTransaction() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter1 = new CounterRef("testCounter5", bucket, antidoteClient);
		CounterRef lowCounter2 = new CounterRef("testCounter3", bucket, antidoteClient);
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter(antidoteTransaction);
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter(antidoteTransaction);
		int oldValue2 = counter2old.getValue();
<<<<<<< HEAD
		antidoteTransaction.commitTransaction();

=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteTransaction tx = antidoteClient.createStaticTransaction();
		counter1old.increment(5, tx);
		counter1old.increment(5, tx);
		counter2old.increment(3, tx);
		counter2old.increment(3, tx);
		tx.commitTransaction();
		tx.close();
<<<<<<< HEAD

		antidoteTransaction = antidoteClient.createTransaction();
=======
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		int newValue1 = counter1old.getValue();
		int newValue2 = counter2old.getValue();
		antidoteTransaction.commitTransaction();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1old.readDatabase(antidoteTransaction);
		counter2old.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
	}

	@Test(timeout=10000)
<<<<<<< HEAD
	public void incBy2Test() {		
		antidoteTransaction = antidoteClient.createTransaction();
=======
	public void incBy2Test() {
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		CounterRef lowCounter = new CounterRef("testCounter5", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
		int oldValue = counter.getValue();
		counter.increment(antidoteTransaction);
		counter.increment(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = counter.getValue();
		assert(newValue == oldValue+2);
		antidoteTransaction = antidoteClient.createTransaction();
		counter.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		newValue = counter.getValue();
		assert(newValue == oldValue+2);
	}

	@Test(timeout=2000)
	public void decrementToZeroTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter testCounter = lowCounter.createAntidoteCounter(antidoteTransaction);
		testCounter.increment(0-testCounter.getValue(), antidoteTransaction);
		assert(testCounter.getValue() == 0); //operation executed locally
		testCounter.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(testCounter.getValue() == 0);
	}

	@Test(timeout=2000)
<<<<<<< HEAD
	public void incBy5Test(){	
		antidoteTransaction = antidoteClient.createTransaction();
=======
	public void incBy5Test(){
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
		int oldValue = counter.getValue();
		counter.increment(5, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = counter.getValue();
		assert(newValue == oldValue+5);
<<<<<<< HEAD
=======
		counter.readDatabase();
		newValue = counter.getValue();
		assert(newValue == oldValue+5);
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}

	@Test(timeout=2000)
	public void addElemTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		ORSetRef lowSet = new ORSetRef("testSet", bucket, antidoteClient);
		AntidoteOuterORSet testSet = lowSet.createAntidoteORSet(antidoteTransaction);
		testSet.addElement("element", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(testSet.getValues().contains("element"));
	}

	@Test(timeout=2000)
	public void remElemTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		ORSetRef lowSet = new ORSetRef("testSet", bucket, antidoteClient);
		AntidoteOuterORSet testSet = lowSet.createAntidoteORSet(antidoteTransaction);
		testSet.addElement(elements, antidoteTransaction);
		testSet.removeElement("Hi", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(! testSet.getValues().contains("Hi"));
		assert(testSet.getValues().contains("Bye"));
	}

	@Test(timeout=2000)
	public void addElemsTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		RWSetRef lowSet = new RWSetRef("testSet1", bucket, antidoteClient);
		List<String> elements = new ArrayList<String>();
		elements.add("Wall");
		elements.add("Ball");
		AntidoteOuterRWSet testSet = lowSet.createAntidoteRWSet(antidoteTransaction);
		testSet.addElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
	}

	@Test(timeout=2000)
	public void remElemsTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		RWSetRef lowSet = new RWSetRef("testSet1", bucket, antidoteClient);
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		AntidoteOuterRWSet testSet = lowSet.createAntidoteRWSet(antidoteTransaction);
		testSet.addElement(elements, antidoteTransaction);
		testSet.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(! testSet.getValues().contains("Hi"));
		assert(! testSet.getValues().contains("Bye"));
		antidoteTransaction = antidoteClient.createTransaction();
		testSet.addElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(testSet.getValues().contains("Hi"));
		assert(testSet.getValues().contains("Bye"));
	}

	@Test(timeout=2000)
	public void updateRegTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		LWWRegisterRef lowReg = new LWWRegisterRef("testReg", bucket, antidoteClient);
<<<<<<< HEAD
        AntidoteOuterLWWRegister testReg = lowReg.createAntidoteLWWRegister(antidoteTransaction);
        testReg.setValue("hi", antidoteTransaction);
        testReg.setValue("bye", antidoteTransaction);
        antidoteTransaction.commitTransaction();
        assert(testReg.getValue().equals("bye"));
        assert(! testReg.getValue().equals("hi"));
=======
		AntidoteOuterLWWRegister testReg = lowReg.createAntidoteLWWRegister();
		testReg.setValue("hi");
		testReg.setValue("bye");
		testReg.push();
		assert(testReg.getValue().equals("bye"));
		assert(! testReg.getValue().equals("hi"));
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}

	@Test(timeout=2000)
	public void updateMVRegTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		MVRegisterRef lowReg = new MVRegisterRef("testReg", bucket, antidoteClient);
<<<<<<< HEAD

		AntidoteOuterMVRegister testReg = lowReg.createAntidoteMVRegister(antidoteTransaction);
        testReg.setValue("hi", antidoteTransaction);
        testReg.setValue("bye", antidoteTransaction);
        antidoteTransaction.commitTransaction();
        assert(testReg.getValueList().contains("bye"));
        assert(! testReg.getValueList().contains("hi"));
=======
		AntidoteOuterMVRegister testReg = lowReg.createAntidoteMVRegister();
		testReg.setValue("hi");
		testReg.setValue("bye");
		testReg.push();
		assert(testReg.getValueList().contains("bye"));
		assert(! testReg.getValueList().contains("hi"));
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}

	@Test(timeout=2000)
	public void incIntBy1Test() {
		antidoteTransaction = antidoteClient.createTransaction();
		IntegerRef lowInt = new IntegerRef("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger(antidoteTransaction);
		int oldValue = integer.getValue();
		integer.increment(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = integer.getValue();
		assert(oldValue+1 == newValue);
	}

	@Test(timeout=2000)
	public void decBy5Test() {
		antidoteTransaction = antidoteClient.createTransaction();
		IntegerRef lowInt = new IntegerRef("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger(antidoteTransaction);
		int oldValue = integer.getValue();
		integer.increment(-5, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = integer.getValue();
		assert(oldValue-5 == newValue);
	}

	@Test(timeout=2000)
<<<<<<< HEAD
	public void setIntTest() {					
		antidoteTransaction = antidoteClient.createTransaction();
		IntegerRef lowInt = new IntegerRef("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger(antidoteTransaction);
		integer.setValue(42, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(integer.getValue() == 42);	
=======
	public void setIntTest() {
		IntegerRef lowInt = new IntegerRef("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger();
		integer.setValue(42);
		integer.push();
		assert(integer.getValue() == 42);
		integer.readDatabase();
		assert(integer.getValue() == 42);
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}
	@Test(timeout=1000)
	public void counterTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String counterKey = "counterKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap12", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement(5);
		testMap.update(counterKey, counterUpdate, antidoteTransaction);
		int counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert (counterValue == 5);
		antidoteTransaction.commitTransaction();
		AntidoteInnerCounter counter = testMap.getCounterEntry(counterKey);
		counterValue = testMap.getCounterEntry(counterKey).getValue();
		assert(counterValue == 5);
		counter = testMap.getCounterEntry(counterKey);
		antidoteTransaction = antidoteClient.createTransaction();
		counter.increment(5, antidoteTransaction);
		counter.increment(5, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(counter.getValue() == 15);
	}

	@Test(timeout=2000)
	public void integerTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String integerKey = "integerKey";
		String mapKey = "mapKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap12", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate integerUpdate = AntidoteMapUpdate.createIntegerIncrement(5);
		AntidoteMapUpdate mapUpdate = AntidoteMapUpdate.createAWMapUpdate(integerKey, integerUpdate);
		testMap.update(mapKey, mapUpdate, antidoteTransaction);
		AntidoteInnerAWMap innerMap = testMap.getAWMapEntry(mapKey);
		int integerValue = innerMap.getIntegerEntry(integerKey).getValue();
		assert (integerValue == 5); 
		antidoteTransaction.commitTransaction();
		innerMap = testMap.getAWMapEntry(mapKey);
		AntidoteInnerInteger integer = innerMap.getIntegerEntry(integerKey);
		integerValue = integer.getValue();
		assert(integerValue == 5);
		antidoteTransaction = antidoteClient.createTransaction();
		integer = innerMap.getIntegerEntry(integerKey);
		integer.increment(5, antidoteTransaction);
		integer.increment(5, antidoteTransaction);
		assert(integer.getValue() == 15);
	}

	@Test(timeout=1000)
	public void registerTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String registerKey = "registerKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("yes");
		testMap.update(registerKey, registerUpdate, antidoteTransaction);
		String registerValue = testMap.getLWWRegisterEntry(registerKey).getValue();
		assert (registerValue.equals("yes")); 
		AntidoteInnerLWWRegister register = testMap.getLWWRegisterEntry(registerKey);
		antidoteTransaction.commitTransaction();
		registerValue = testMap.getLWWRegisterEntry(registerKey).getValue();
		assert(registerValue.equals("yes")); 
		register = testMap.getLWWRegisterEntry(registerKey);
<<<<<<< HEAD
		antidoteTransaction = antidoteClient.createStaticTransaction();
		register.setValue("no", antidoteTransaction);
		register.setValue("maybe", antidoteTransaction);
		assert(register.getValue().equals("maybe"));
		antidoteTransaction.commitTransaction();
=======
		register.setValue("no");
		register.setValue("maybe");
		assert(register.getValue().equals("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValue().equals("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.remove(registerKey, AntidoteType.LWWRegisterType);
		testMap.push(); // everything set to initial situation
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}
	@Test(timeout=2000)
	public void mvRegisterTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String registerKey = "mvRegisterKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createMVRegisterSet("yes");
		testMap.update(registerKey, registerUpdate, antidoteTransaction);
		List<String> registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert (registerValueList.contains("yes"));
		AntidoteInnerMVRegister register = testMap.getMVRegisterEntry(registerKey);
		antidoteTransaction.commitTransaction();
		registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		assert(registerValueList.contains("yes")); 
		register = testMap.getMVRegisterEntry(registerKey);
<<<<<<< HEAD
		antidoteTransaction = antidoteClient.createTransaction();
		register.setValue("no", antidoteTransaction);
		register.setValueBS(ByteString.copyFromUtf8("maybe"), antidoteTransaction);
		assert(register.getValueList().contains("maybe"));
		antidoteTransaction.commitTransaction();
=======
		register.setValue("no");
		register.setValueBS(ByteString.copyFromUtf8("maybe"));
		assert(register.getValueList().contains("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValueList().contains("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.remove(registerKey, AntidoteType.MVRegisterType);
		testMap.push(); // everything set to initial situation
>>>>>>> ed52f730f46633262b88654417000a87188347f6
	}

	@Test(timeout=2000)
	public void orSetTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String setKey = "orSetKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate setUpdate = AntidoteMapUpdate.createORSetAdd("yes");
		testMap.update(setKey, setUpdate, antidoteTransaction);
		Set <String> setValueList = testMap.getORSetEntry(setKey).getValues();
		assert (setValueList.contains("yes")); //local value is "yes"
		AntidoteInnerORSet set = testMap.getORSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getORSetEntry(setKey).getValues();
		assert(setValueList.contains("yes")); //update forwarded to database, then got a new state from database
		set = testMap.getORSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(! set.getValues().contains("maybe"));
		assert(set.getValues().contains("no"));// 3 local updates in a row
	}

	@Test(timeout=2000)
	public void rwSetTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		String setKey = "rwSetKey";
		AWMapRef lowMap = new AWMapRef("testMapBestMap3", bucket, antidoteClient);
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		AntidoteMapUpdate setUpdate = AntidoteMapUpdate.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate, antidoteTransaction);
		Set <String> setValueList = testMap.getRWSetEntry(setKey).getValues();
		assert (setValueList.contains("yes"));
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getRWSetEntry(setKey).getValues();
		assert(setValueList.contains("yes"));
		set = testMap.getRWSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(! set.getValues().contains("maybe"));
		assert(set.getValues().contains("no"));
	}

	@Test(timeout=2000)
	public void rwSetTest4() {
		antidoteTransaction = antidoteClient.createTransaction();
		String setKey = "rwSetKey";
		GMapRef lowMap = new GMapRef("testMapBestMap4", bucket, antidoteClient);
		AntidoteOuterGMap testMap = lowMap.createAntidoteGMap(antidoteTransaction);
		AntidoteMapUpdate setUpdate = AntidoteMapUpdate.createRWSetAdd("yes");
		testMap.update(setKey, setUpdate, antidoteTransaction);
		Set <String> setValueList = testMap.getRWSetEntry(setKey).getValues();
		assert (setValueList.contains("yes"));
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getRWSetEntry(setKey).getValues();
		assert(setValueList.contains("yes"));
		set = testMap.getRWSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		assert(! set.getValues().contains("maybe"));
		assert(set.getValues().contains("no"));
	}

	@Test(timeout=10000)
	public void createRemoveTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		AWMapRef lowMap = new AWMapRef("emptyMapBestMap", bucket, antidoteClient);
<<<<<<< HEAD
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap(antidoteTransaction);
		
=======
		AntidoteOuterAWMap testMap = lowMap.createAntidoteAWMap();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		String orSetKey = "tempORSetKey";
		String rwSetKey = "tempRWSetKey";
		String counterKey = "tempCounterKey";
		String integerKey = "tempIntegerKey";
		String registerKey = "tempRegisterKey";
		String mvRegisterKey = "tempMVRegisterKey";
		String innerAWMapKey = "tempAWMapKey";
		String awMapKey = "awMapKey";
		String gMapKey = "gMapKey";
		AntidoteMapUpdate orSetUpdate = AntidoteMapUpdate.createORSetAdd("yes");
		AntidoteMapUpdate rwSetUpdate = AntidoteMapUpdate.createRWSetAdd("yes");
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement();
		AntidoteMapUpdate integerUpdate = AntidoteMapUpdate.createIntegerIncrement();
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("yes");
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("yes");
		AntidoteMapUpdate innerAWMapUpdate = AntidoteMapUpdate.createAWMapUpdate(orSetKey, orSetUpdate);
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate(orSetKey, orSetUpdate);
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(orSetKey, orSetUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(rwSetKey, rwSetUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(counterKey, counterUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(integerKey, integerUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(registerKey, registerUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(mvRegisterKey, mvRegisterUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(innerAWMapKey, innerAWMapUpdate);
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		awMapUpdate = AntidoteMapUpdate.createAWMapUpdate(gMapKey, gMapUpdate);
<<<<<<< HEAD
		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		
		antidoteTransaction.commitTransaction();
=======
		testMap.update(awMapKey, awMapUpdate);
>>>>>>> ed52f730f46633262b88654417000a87188347f6

		testMap.synchronize();
		AntidoteMapUpdate orSetRemove = AntidoteMapUpdate.createMapRemove(orSetKey, AntidoteType.ORSetType);
		AntidoteMapUpdate rwSetRemove = AntidoteMapUpdate.createMapRemove(rwSetKey, AntidoteType.RWSetType);
		AntidoteMapUpdate counterRemove = AntidoteMapUpdate.createMapRemove(counterKey, AntidoteType.CounterType);
		AntidoteMapUpdate integerRemove = AntidoteMapUpdate.createMapRemove(integerKey, AntidoteType.IntegerType);
		AntidoteMapUpdate registerRemove = AntidoteMapUpdate.createMapRemove(registerKey, AntidoteType.LWWRegisterType);
		AntidoteMapUpdate mvRegisterRemove = AntidoteMapUpdate.createMapRemove(mvRegisterKey, AntidoteType.MVRegisterType);
		AntidoteMapUpdate awMapRemove = AntidoteMapUpdate.createMapRemove(innerAWMapKey, AntidoteType.AWMapType);
		AntidoteMapUpdate gMapRemove = AntidoteMapUpdate.createMapRemove(gMapKey, AntidoteType.GMapType);
<<<<<<< HEAD

		antidoteTransaction = antidoteClient.createTransaction();
		
		testMap.update(awMapKey, orSetRemove, antidoteTransaction);
		testMap.update(awMapKey, rwSetRemove, antidoteTransaction);
		testMap.update(awMapKey, counterRemove, antidoteTransaction);
		testMap.update(awMapKey, integerRemove, antidoteTransaction);
		testMap.update(awMapKey, registerRemove, antidoteTransaction);
		testMap.update(awMapKey, mvRegisterRemove, antidoteTransaction);
		testMap.update(awMapKey, awMapRemove, antidoteTransaction);
		testMap.update(awMapKey, gMapRemove,antidoteTransaction);

		antidoteTransaction.commitTransaction();
=======
		testMap.update(awMapKey, orSetRemove);
		testMap.update(awMapKey, rwSetRemove);
		testMap.update(awMapKey, counterRemove);
		testMap.update(awMapKey, integerRemove);
		testMap.update(awMapKey, registerRemove);
		testMap.update(awMapKey, mvRegisterRemove);
		testMap.update(awMapKey, awMapRemove);
		testMap.update(awMapKey, gMapRemove);
>>>>>>> ed52f730f46633262b88654417000a87188347f6

		testMap.synchronize();
		AntidoteInnerAWMap innerMap = testMap.getAWMapEntry(awMapKey);

		assert(innerMap.getEntryList().size()==0);
	}

	@Test(timeout=10000)
	public void updateTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		IntegerRef lowInteger = new IntegerRef("testInteger", bucket, antidoteClient);
		LWWRegisterRef lowLWWRegister = new LWWRegisterRef("testRegister", bucket, antidoteClient);
		MVRegisterRef lowMVRegister = new MVRegisterRef("testMVRegister", bucket, antidoteClient);
		ORSetRef lowORSet = new ORSetRef("testORSet", bucket, antidoteClient);
		RWSetRef lowRWSet = new RWSetRef("testRWSet", bucket, antidoteClient);
		AWMapRef lowAWMap = new AWMapRef("testAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("testGMap", bucket, antidoteClient);
<<<<<<< HEAD

		lowCounter.increment(2, antidoteTransaction);
		lowInteger.set(7, antidoteTransaction);
		lowInteger.increment(1, antidoteTransaction);
		lowLWWRegister.set("Hi", antidoteTransaction);
		lowMVRegister.set("Hi", antidoteTransaction);

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
		
=======
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

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.LWWRegisterType, "testRegister");
		AntidoteMapKey mvRegisterKey = new AntidoteMapKey(AntidoteType.MVRegisterType, "testMVRegister");
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement();
		AntidoteMapUpdate intSet = AntidoteMapUpdate.createIntegerSet(3);
		AntidoteMapUpdate intInc = AntidoteMapUpdate.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetAdd = AntidoteMapUpdate.createRWSetAdd("Hi");
		AntidoteMapUpdate rwSetAdd2 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate rwSetAdd3 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate orSetAdd = AntidoteMapUpdate.createORSetAdd("Hi");
		AntidoteMapUpdate orSetAdd2 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetAdd3 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate rwSetRemove = AntidoteMapUpdate.createRWSetRemove("Hi");
		AntidoteMapUpdate rwSetRemove2 = AntidoteMapUpdate.createRWSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetRemove = AntidoteMapUpdate.createORSetRemove("Hi");
		AntidoteMapUpdate orSetRemove2 = AntidoteMapUpdate.createORSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("Hi");
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("testCounter", counterUpdate);
<<<<<<< HEAD
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
		
=======
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
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(counter.getValue() == 2);
		assert(integer.getValue() == 8);
		assert(register.getValue().equals("Hi"));
		assert(mvRegister.getValueList().contains("Hi"));
		assert(orSet.getValues().contains("Hi2"));
		assert(! orSet.getValues().contains("Hi"));
		assert(! orSet.getValues().contains("Hi3"));
		assert(rwSet.getValues().contains("Hi2"));
		assert(! rwSet.getValues().contains("Hi"));
		assert(! rwSet.getValues().contains("Hi3"));
		assert(awMap.getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getIntegerEntry("testInteger").getValue() == 5);
		assert(awMap.getORSetEntry("testORSet").getValues().contains("Hi3"));
		assert(awMap.getRWSetEntry("testRWSet").getValues().contains("Hi3"));
		assert(awMap.getLWWRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
<<<<<<< HEAD
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowAWMap.remove(counterKey, antidoteTransaction);
		lowLWWRegister.setBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		lowMVRegister.setBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		
		register.readDatabase(antidoteTransaction);
		mvRegister.readDatabase(antidoteTransaction);
		awMap.readDatabase(antidoteTransaction);
		
=======

		lowAWMap.remove(counterKey);
		lowLWWRegister.setBS(ByteString.copyFromUtf8("Hi2"));
		lowMVRegister.setBS(ByteString.copyFromUtf8("Hi2"));

		register.readDatabase();
		mvRegister.readDatabase();
		awMap.readDatabase();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(register.getValue().equals("Hi2"));
		assert(mvRegister.getValueList().contains("Hi2"));
		assert(awMap.getCounterEntry("testCounter") == null);
	}

	@Test(timeout=10000)
	public void transactionTest() {
		LWWRegisterRef lowLWWRegister = new LWWRegisterRef("testRegister", bucket, antidoteClient);
		MVRegisterRef lowMVRegister = new MVRegisterRef("testMVRegister", bucket, antidoteClient);
		ORSetRef lowORSet = new ORSetRef("testORSet", bucket, antidoteClient);
		RWSetRef lowRWSet = new RWSetRef("testRWSet", bucket, antidoteClient);
		AWMapRef lowAWMap = new AWMapRef("testAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("testGMap", bucket, antidoteClient);

		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.LWWRegisterType, "testRegister");
		AntidoteMapKey mvRegisterKey = new AntidoteMapKey(AntidoteType.MVRegisterType, "testMVRegister");
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement();
		AntidoteMapUpdate intSet = AntidoteMapUpdate.createIntegerSet(3);
		AntidoteMapUpdate intInc = AntidoteMapUpdate.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetAdd = AntidoteMapUpdate.createRWSetAdd("Hi");
		AntidoteMapUpdate rwSetAdd2 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate rwSetAdd3 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate orSetAdd = AntidoteMapUpdate.createORSetAdd("Hi");
		AntidoteMapUpdate orSetAdd2 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetAdd3 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate rwSetRemove = AntidoteMapUpdate.createRWSetRemove("Hi");
		AntidoteMapUpdate rwSetRemove2 = AntidoteMapUpdate.createRWSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetRemove = AntidoteMapUpdate.createORSetRemove("Hi");
		AntidoteMapUpdate orSetRemove2 = AntidoteMapUpdate.createORSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet(ByteString.copyFromUtf8("Hi"));
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("testCounter", counterUpdate);
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		IntegerRef lowInteger = new IntegerRef("testInteger", bucket, antidoteClient);
<<<<<<< HEAD
		lowCounter.increment(2, antidoteTransaction);
		lowInteger.set(7, antidoteTransaction);
		lowInteger.increment(1, antidoteTransaction);
		
		lowORSet.add("Hi", antidoteTransaction);
		lowRWSet.add("Hi", antidoteTransaction);
=======
		//	lowCounter.increment(2, antidoteTransaction);
		//	lowInteger.set(7, antidoteTransaction);
		//	lowInteger.increment(1, antidoteTransaction);

		//	lowORSet.add("Hi", antidoteTransaction);
	/*	lowRWSet.add("Hi", antidoteTransaction);
>>>>>>> ed52f730f46633262b88654417000a87188347f6
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
		assert(orSet.getValues().contains("Hi2"));
		assert(! orSet.getValues().contains("Hi"));
		assert(! orSet.getValues().contains("Hi3"));
		assert(rwSet.getValues().contains("Hi2"));
		assert(! rwSet.getValues().contains("Hi"));
		assert(! rwSet.getValues().contains("Hi3"));
		assert(awMap.getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getIntegerEntry("testInteger").getValue() == 5);
		assert(awMap.getORSetEntry("testORSet").getValues().contains("Hi3"));
		assert(awMap.getRWSetEntry("testRWSet").getValues().contains("Hi3"));
		assert(awMap.getLWWRegisterEntry("testRegister").getValue().equals("Hi"));
		assert(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		assert(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue() == 1);
		assert(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue() == 1);
		assert(gMap.getCounterEntry("testCounter").getValue() == 1);
	}

	@Test(timeout=2000)
	public void counterTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement(5);
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("counterKey", counterUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerGMap", gMapUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerGMap testGMap3 = testAWMap2.getGMapEntry("innerGMap");
		AntidoteInnerCounter counter1 = testAWMap.getCounterEntry("counterKey");
		AntidoteInnerCounter counter2 = testAWMap2.getCounterEntry("counterKey");
		AntidoteInnerCounter counter3 = testGMap3.getCounterEntry("counterKey");
<<<<<<< HEAD

		counter1.increment(1, antidoteTransaction);
		counter2.increment(2, antidoteTransaction);
		counter3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		assert(counter1.getValue()==6);
		assert(counter2.getValue()==7);
		assert(counter3.getValue()==8);
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======
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
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerAWMap testAWMap3 = testGMap2.getAWMapEntry("innerAWMap");
		counter1 = testGMap.getCounterEntry("counterKey");
		counter2 = testGMap2.getCounterEntry("counterKey");
		counter3 = testAWMap3.getCounterEntry("counterKey");
<<<<<<< HEAD

		counter1.increment(1, antidoteTransaction);
		counter2.increment(2, antidoteTransaction);
		counter3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();
=======
		counter1.increment(1);
		counter2.increment(2);
		counter3.increment(3);
		counter1.synchronize();
		counter2.synchronize();
		counter3.synchronize();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(counter1.getValue()==6);
		assert(counter2.getValue()==7);
		assert(counter3.getValue()==8);
	}

	@Test(timeout=2000)
	public void integerTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate integerUpdate = AntidoteMapUpdate.createIntegerIncrement(5);
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerInteger integer1 = testAWMap.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer2 = testAWMap2.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer3 = testAWMap3.getIntegerEntry("integerKey");
<<<<<<< HEAD

		integer1.increment(1, antidoteTransaction);
		integer2.increment(2, antidoteTransaction);
		integer3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		assert(integer1.getValue()==6);
		assert(integer2.getValue()==7);
		assert(integer3.getValue()==8);
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======
		integer1.increment(1);
		integer2.increment(2);
		integer3.increment(3);
		integer1.synchronize();
		integer2.synchronize();
		integer3.synchronize();

		assert(integer1.getValue()==6);
		assert(integer2.getValue()==7);
		assert(integer3.getValue()==8);

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);

		lowGMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		integer1 = testGMap.getIntegerEntry("integerKey");
		integer2 = testGMap2.getIntegerEntry("integerKey");
		integer3 = testGMap3.getIntegerEntry("integerKey");
<<<<<<< HEAD

		integer1.increment(1, antidoteTransaction);
		integer2.increment(2, antidoteTransaction);
		integer3.increment(3, antidoteTransaction);

		antidoteTransaction.commitTransaction();
=======
		integer1.increment(1);
		integer2.increment(2);
		integer3.increment(3);
		integer1.synchronize();
		integer2.synchronize();
		integer3.synchronize();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(integer1.getValue()==6);
		assert(integer2.getValue()==7);
		assert(integer3.getValue()==8);
	}

	@Test(timeout=3000)
	public void orSetTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate orSetUpdate = AntidoteMapUpdate.createORSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerORSet orSet1 = testAWMap.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet2 = testAWMap2.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet3 = testAWMap3.getORSetEntry("orSetKey");
<<<<<<< HEAD

		orSet1.addElement("Hi2", antidoteTransaction);
		orSet2.addElement("Hi3", antidoteTransaction);
		orSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		assert(orSet1.getValues().contains("Hi2"));
		assert(orSet2.getValues().contains("Hi3"));
		assert(orSet3.getValues().contains("Hi4"));
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======
		orSet1.addElement("Hi2");
		orSet2.addElement("Hi3");
		orSet3.addElement("Hi4");
		orSet1.synchronize();
		orSet2.synchronize();
		orSet3.synchronize();

		assert(orSet1.getValues().contains("Hi2"));
		assert(orSet2.getValues().contains("Hi3"));
		assert(orSet3.getValues().contains("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);

		lowGMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		orSet1 = testGMap.getORSetEntry("orSetKey");
		orSet2 = testGMap2.getORSetEntry("orSetKey");
		orSet3 = testGMap3.getORSetEntry("orSetKey");
<<<<<<< HEAD

		orSet1.addElement("Hi2", antidoteTransaction);
		orSet2.addElement("Hi3", antidoteTransaction);
		orSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
=======
		orSet1.addElement("Hi2");
		orSet2.addElement("Hi3");
		orSet3.addElement("Hi4");
		orSet1.synchronize();
		orSet2.synchronize();
		orSet3.synchronize();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(orSet1.getValues().contains("Hi2"));
		assert(orSet2.getValues().contains("Hi3"));
		assert(orSet3.getValues().contains("Hi4"));
	}

	@Test(timeout=2000)
	public void rwSetTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate rwSetUpdate = AntidoteMapUpdate.createRWSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerRWSet rwSet1 = testAWMap.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet2 = testAWMap2.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet3 = testAWMap3.getRWSetEntry("rwSetKey");
<<<<<<< HEAD

		rwSet1.addElement("Hi2", antidoteTransaction);
		rwSet2.addElement("Hi3", antidoteTransaction);
		rwSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
=======
		rwSet1.addElement("Hi2");
		rwSet2.addElement("Hi3");
		rwSet3.addElement("Hi4");
		rwSet1.synchronize();
		rwSet2.synchronize();
		rwSet3.synchronize();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(rwSet1.getValues().contains("Hi2"));
		assert(rwSet2.getValues().contains("Hi3"));
		assert(rwSet3.getValues().contains("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
<<<<<<< HEAD
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======

		lowGMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		rwSet1 = testGMap.getRWSetEntry("rwSetKey");
		rwSet2 = testGMap2.getRWSetEntry("rwSetKey");
		rwSet3 = testGMap3.getRWSetEntry("rwSetKey");
<<<<<<< HEAD

		rwSet1.addElement("Hi2", antidoteTransaction);
		rwSet2.addElement("Hi3", antidoteTransaction);
		rwSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
=======
		rwSet1.addElement("Hi2");
		rwSet2.addElement("Hi3");
		rwSet3.addElement("Hi4");
		rwSet1.synchronize();
		rwSet2.synchronize();
		rwSet3.synchronize();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(rwSet1.getValues().contains("Hi2"));
		assert(rwSet2.getValues().contains("Hi3"));
		assert(rwSet3.getValues().contains("Hi4"));
	}

	@Test(timeout=2000)
	public void registerTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerLWWRegister register1 = testAWMap.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register2 = testAWMap2.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register3 = testAWMap3.getLWWRegisterEntry("registerKey");
<<<<<<< HEAD

		register1.setValueBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		register2.setValueBS(ByteString.copyFromUtf8("Hi3"), antidoteTransaction);
		register3.setValueBS(ByteString.copyFromUtf8("Hi4"), antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		assert(register1.getValue().equals("Hi2"));
		assert(register2.getValue().equals("Hi3"));
		assert(register3.getValue().equals("Hi4"));
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======
		register1.setValueBS(ByteString.copyFromUtf8("Hi2"));
		register2.setValueBS(ByteString.copyFromUtf8("Hi3"));
		register3.setValueBS(ByteString.copyFromUtf8("Hi4"));
		register1.synchronize();
		register2.synchronize();
		register3.synchronize();

		assert(register1.getValue().equals("Hi2"));
		assert(register2.getValue().equals("Hi3"));
		assert(register3.getValue().equals("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);

		lowGMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		register1 = testGMap.getLWWRegisterEntry("registerKey");
		register2 = testGMap2.getLWWRegisterEntry("registerKey");
		register3 = testGMap3.getLWWRegisterEntry("registerKey");
<<<<<<< HEAD

		register1.setValue("Hi2", antidoteTransaction);
		register2.setValue("Hi3", antidoteTransaction);
		register3.setValue("Hi4", antidoteTransaction);

		antidoteTransaction.commitTransaction();
=======
		register1.setValue("Hi2");
		register2.setValue("Hi3");
		register3.setValue("Hi4");
		register1.synchronize();
		register2.synchronize();
		register3.synchronize();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(register1.getValue().equals("Hi2"));
		assert(register2.getValue().equals("Hi3"));
		assert(register3.getValue().equals("Hi4"));
	}

	@Test(timeout=2000)
	public void mvRegisterTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
<<<<<<< HEAD
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
=======

		lowAWMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2);
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerMVRegister mvRegister1 = testAWMap.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister2 = testAWMap2.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister3 = testAWMap3.getMVRegisterEntry("mvRegisterKey");
<<<<<<< HEAD

		mvRegister1.setValue("Hi2", antidoteTransaction);
		mvRegister2.setValueBS(ByteString.copyFromUtf8("Hi3"), antidoteTransaction);
		mvRegister3.setValueBS(ByteString.copyFromUtf8("Hi4"), antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
=======
		mvRegister1.setValueBS(ByteString.copyFromUtf8("Hi2"));
		mvRegister2.setValueBS(ByteString.copyFromUtf8("Hi3"));
		mvRegister3.setValueBS(ByteString.copyFromUtf8("Hi4"));
		mvRegister1.synchronize();
		mvRegister2.synchronize();
		mvRegister3.synchronize();

>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(mvRegister1.getValueList().contains("Hi2"));
		assert(mvRegister2.getValueList().contains("Hi3"));
		assert(mvRegister3.getValueList().contains("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
<<<<<<< HEAD
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
				
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);
=======

		lowGMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		mvRegister1 = testGMap.getMVRegisterEntry("mvRegisterKey");
		mvRegister2 = testGMap2.getMVRegisterEntry("mvRegisterKey");
		mvRegister3 = testGMap3.getMVRegisterEntry("mvRegisterKey");
<<<<<<< HEAD

		mvRegister1.setValue("Hi2", antidoteTransaction);
		mvRegister2.setValue("Hi3", antidoteTransaction);
		mvRegister3.setValue("Hi4", antidoteTransaction);

		antidoteTransaction.commitTransaction();
=======
		mvRegister1.setValue("Hi2");
		mvRegister2.setValue("Hi3");
		mvRegister3.setValue("Hi4");
		mvRegister1.synchronize();
		mvRegister2.synchronize();
		mvRegister3.synchronize();
>>>>>>> ed52f730f46633262b88654417000a87188347f6
		assert(mvRegister1.getValueList().contains("Hi2"));
		assert(mvRegister2.getValueList().contains("Hi3"));
		assert(mvRegister3.getValueList().contains("Hi4"));
	}
}