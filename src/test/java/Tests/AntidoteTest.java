package test.java.Tests;
import main.java.AntidoteClient.*;

import java.io.File;
import java.security.SecureRandom;
import java.math.BigInteger;
import java.util.*;
import java.util.ArrayList;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import com.google.protobuf.ByteString;
import static org.hamcrest.CoreMatchers.containsString;

public class AntidoteTest{
	PoolManager antidotePoolManager;
	AntidoteClient antidoteClient;
	String bucket;
	AntidoteTransaction antidoteTransaction;

	public AntidoteTest() {
		AntidoteConfigManager cfgMgr = new AntidoteConfigManager();
		if (!cfgMgr.configExist()) {
			cfgMgr.generateDefaultConfig();
		}
		List<Host> hosts = cfgMgr.getConfigHosts();
		if (hosts.size() == 0) {
			// Should throuw error here
			System.out.println("NO HOSTS FOUND!!!");
		}
		antidotePoolManager = new PoolManager(20, 5, hosts);
		antidoteClient = new AntidoteClient(antidotePoolManager);
		bucket = nextSessionId();
	}

	public String nextSessionId() {
		SecureRandom random = new SecureRandom();
		return new BigInteger(130, random).toString(32);
	}
	@Test(timeout=10000)
	public void defaultConfigGeneration() {
		String fileName = "config.xml";
		AntidoteConfigManager cfgMgr = new AntidoteConfigManager();
		if (!cfgMgr.configExist()) {
			cfgMgr.generateDefaultConfig();
			String path = System.getProperty("user.dir") + "/" + fileName;
			File tmpFile = new File(path);
			Assert.assertEquals(tmpFile.exists(), true);
		}
	}
	@Test(timeout=10000)
	public void configHosts() {
		AntidoteConfigManager cfgMgr = new AntidoteConfigManager();
		if (!cfgMgr.configExist()) {
			cfgMgr.generateDefaultConfig();
		}
		List<Host> hosts = cfgMgr.getConfigHosts();
		Assert.assertTrue(hosts.size() > 0);
	}
	@Test(timeout=10000)
	public void readStaticTransaction() {
		List<ObjectRef> objectRefs = new ArrayList<>();
		CounterRef lowCounter = antidoteClient.counterRef("testCounter", bucket);
		CounterRef lowCounter1 = antidoteClient.counterRef("testCounter1", bucket);
		IntegerRef lowInt =antidoteClient.integerRef("testInteger", bucket);
		IntegerRef lowInt1 = antidoteClient.integerRef("testInteger1", bucket);
		ORSetRef orSetRef = antidoteClient.orSetRef("testorSetRef", bucket);
		ORSetRef orSetRef1 = antidoteClient.orSetRef("testorSetRef1", bucket);
		RWSetRef rwSetRef = antidoteClient.rwSetRef("testrwSetRef", bucket);
		RWSetRef rwSetRef1 = antidoteClient.rwSetRef("testrwSetRef1", bucket);
		MVRegisterRef mvRegisterRef = antidoteClient.mvRegisterRef("testMvRegisterRef",bucket);
		MVRegisterRef mvRegisterRef1 = antidoteClient.mvRegisterRef("testMvRegisterRef1",bucket);
		LWWRegisterRef lwwRegisterRef = antidoteClient.lwwRegisterRef("testLwwRegisterRef",bucket);
		LWWRegisterRef lwwRegisterRef1 = antidoteClient.lwwRegisterRef("testLwwRegisterRef1",bucket);
		AWMapRef awMapRef = antidoteClient.awMapRef("testawMapRef",bucket);

		AntidoteOuterInteger integer = lowInt1.createAntidoteInteger();
		AntidoteOuterCounter counter = lowCounter1.createAntidoteCounter();
		AntidoteOuterORSet orSet2 = orSetRef.createAntidoteORSet();
		AntidoteOuterORSet orSet = orSetRef1.createAntidoteORSet();
		AntidoteOuterRWSet rwSet = rwSetRef1.createAntidoteRWSet();
		AntidoteOuterRWSet rwSet2 = rwSetRef.createAntidoteRWSet();
		AntidoteOuterMVRegister mvRegister = mvRegisterRef1.createAntidoteMVRegister();
		AntidoteOuterMVRegister mvRegister2 = mvRegisterRef.createAntidoteMVRegister();
		AntidoteOuterLWWRegister lwwRegister = lwwRegisterRef1.createAntidoteLWWRegister();
		AntidoteOuterLWWRegister lwwRegister2 = lwwRegisterRef.createAntidoteLWWRegister();
		AntidoteOuterAWMap awMap = awMapRef.createAntidoteAWMap();

		AntidoteTransaction tx = antidoteClient.createTransaction();
		lowInt.increment(3,tx);
		lowCounter.increment(4,tx);
		orSetRef.add("Hi",tx);
		orSetRef.add("Bye",tx);
		orSetRef.add("yo",tx);
		rwSetRef.add("Hi2",tx);
		rwSetRef.add("Bye2",tx);
		mvRegisterRef.set("mvValue1",tx);
		mvRegisterRef.set("mvValue2",tx);
		lwwRegisterRef.set("lwwValue1",tx);
		tx.commitTransaction();
		tx.close();

		objectRefs.add(lowInt);
		objectRefs.add(lowCounter);
		objectRefs.add(orSetRef);
		objectRefs.add(rwSetRef);
		objectRefs.add(mvRegisterRef);
		objectRefs.add(lwwRegisterRef);

		List<Object> objects = antidoteClient.readObjects(objectRefs);

		AntidoteTransaction tx1 = antidoteClient.createTransaction();
		integer.increment(1,tx1);
		counter.increment(2,tx1);
		orSet.addElement("hi",tx1);
		orSet.addElement("bye",tx1);
		orSet.addElement("ciao",tx1);
		rwSet.addElement("hi2",tx1);
		rwSet.addElement("bye2",tx1);
		mvRegister.setValue("mvValue",tx1);
		lwwRegister.setValue("lwwValue",tx1);
		orSet2.addElementBS((List<ByteString>)objects.get(2),tx1);
		rwSet2.addElementBS((List<ByteString>)objects.get(3),tx1);
		lwwRegister2.setValueBS((ByteString) objects.get(5),tx1);
		tx1.commitTransaction();
		tx1.close();

		antidoteClient.readOuterObjects(Arrays.asList(integer, counter, orSet, rwSet, mvRegister, lwwRegister, orSet2, rwSet2,mvRegister2, lwwRegister2));

		Assert.assertEquals(objects.get(0), 3);
		Assert.assertEquals(objects.get(1), 4);
		Assert.assertEquals(integer.getValue(), 1);
		Assert.assertEquals(integer.getValue(), 2);
		Assert.assertTrue(orSet.getValues().contains("hi"));
		Assert.assertTrue(orSet.getValues().contains("bye"));
		Assert.assertTrue(orSet.getValues().contains("ciao"));
		Assert.assertTrue(rwSet.getValues().contains("hi2"));
		Assert.assertTrue(orSet2.getValues().contains("Bye"));
		Assert.assertTrue(orSet2.getValues().contains("Hi"));
		Assert.assertTrue(orSet2.getValues().contains("yo"));
		Assert.assertTrue(rwSet2.getValues().contains("Hi2"));
		Assert.assertTrue(rwSet2.getValues().contains("Bye2"));
		Assert.assertTrue(rwSet.getValues().contains("bye2"));
		Assert.assertTrue(mvRegister.getValueList().contains("mvValue"));
		Assert.assertTrue(lwwRegister.getValue().contains("lwwValue"));
		Assert.assertTrue(lwwRegister2.getValue().contains("lwwValue1"));

		AntidoteMapKey integerKey = new AntidoteMapKey(AntidoteType.IntegerType, "testInteger");
		AntidoteMapKey counterKey = new AntidoteMapKey(AntidoteType.CounterType, "testCounter");
		AntidoteMapKey orSetKey = new AntidoteMapKey(AntidoteType.ORSetType, "testORSet");
		AntidoteMapKey rwSetKey = new AntidoteMapKey(AntidoteType.RWSetType, "testRWSet");
		AntidoteMapKey awMapKey = new AntidoteMapKey(AntidoteType.AWMapType, "testAWMap");
		AntidoteMapKey gMapKey = new AntidoteMapKey(AntidoteType.GMapType, "testGMap");
		AntidoteMapKey registerKey = new AntidoteMapKey(AntidoteType.LWWRegisterType, "testRegister");
		AntidoteMapKey mvRegisterKey = new AntidoteMapKey(AntidoteType.MVRegisterType, "testMVRegister");
		AntidoteMapUpdate counterUpdate = AntidoteMapUpdate.createCounterIncrement(5);
		AntidoteMapUpdate intSet = AntidoteMapUpdate.createIntegerSet(6);
		AntidoteMapUpdate intInc = AntidoteMapUpdate.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetAdd = AntidoteMapUpdate.createRWSetAdd("Hi");
		AntidoteMapUpdate rwSetAdd2 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate rwSetAdd3 = AntidoteMapUpdate.createRWSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate orSetAdd = AntidoteMapUpdate.createORSetAdd("Hi");
		AntidoteMapUpdate orSetAdd2 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetAdd3 = AntidoteMapUpdate.createORSetAddBS(ByteString.copyFromUtf8("Hi3"));
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("Hi");
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("testCounter", counterUpdate);

		AntidoteTransaction tx2 = antidoteClient.createTransaction();

		awMapRef.update(counterKey,counterUpdate,tx2);
		awMapRef.update(integerKey, intSet, tx2);
		awMapRef.update(integerKey, intInc, tx2);
		awMapRef.update(orSetKey, orSetAdd, tx2);
		awMapRef.update(orSetKey, orSetAdd2, tx2);
		awMapRef.update(orSetKey, orSetAdd3, tx2);
		awMapRef.update(rwSetKey, rwSetAdd, tx2);
		awMapRef.update(rwSetKey, rwSetAdd2, tx2);
		awMapRef.update(rwSetKey, rwSetAdd3, tx2);
		awMapRef.update(registerKey, registerUpdate, tx2);
		awMapRef.update(mvRegisterKey, mvRegisterUpdate, tx2);
		awMapRef.update(awMapKey, awMapUpdate, tx2);
		awMapRef.update(gMapKey, gMapUpdate, tx2);

		tx2.commitTransaction();
		tx2.close();

		antidoteClient.readOuterObjects(Arrays.asList(awMap));

		Assert.assertEquals(awMap.getCounterEntry("testCounter").getValue(), 5);
		Assert.assertEquals(awMap.getIntegerEntry("testInteger").getValue(), 8);
		Assert.assertTrue(awMap.getORSetEntry("testORSet").getValues().contains("Hi3"));
		Assert.assertTrue(awMap.getRWSetEntry("testRWSet").getValues().contains("Hi3"));
		Assert.assertEquals(awMap.getLWWRegisterEntry("testRegister").getValue(), "Hi");
		Assert.assertTrue(awMap.getMVRegisterEntry("testMVRegister").getValueList().contains("Hi"));
		Assert.assertEquals(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue(), 5);
		Assert.assertEquals(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue(), 5);

		AntidoteMapUpdate counterUpdate1 = AntidoteMapUpdate.createCounterIncrement(5);
		AntidoteMapUpdate intInc2 = AntidoteMapUpdate.createIntegerIncrement(2);
		AntidoteMapUpdate rwSetRemove = AntidoteMapUpdate.createRWSetRemove("Hi");
		AntidoteMapUpdate rwSetRemove2 = AntidoteMapUpdate.createRWSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate orSetRemove = AntidoteMapUpdate.createORSetRemove("Hi");
		AntidoteMapUpdate orSetRemove2 = AntidoteMapUpdate.createORSetRemoveBS(ByteString.copyFromUtf8("Hi2"));
		AntidoteMapUpdate registerUpdate2 = AntidoteMapUpdate.createRegisterSet("Hi2");
		AntidoteMapUpdate mvRegisterUpdate2 = AntidoteMapUpdate.createMVRegisterSet("Hi2");
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("testCounter", counterUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("testCounter", counterUpdate);

		AntidoteTransaction tx3 = antidoteClient.createTransaction();

		awMapRef.update(counterKey,counterUpdate1,tx3);
		awMapRef.update(integerKey, intInc2, tx3);
		awMapRef.update(orSetKey, orSetRemove, tx3);
		awMapRef.update(orSetKey, orSetRemove2, tx3);
		awMapRef.update(rwSetKey, rwSetRemove, tx3);
		awMapRef.update(rwSetKey, rwSetRemove2, tx3);
		awMapRef.update(registerKey, registerUpdate2, tx3);
		awMapRef.update(mvRegisterKey, mvRegisterUpdate2, tx3);
		awMapRef.update(awMapKey, awMapUpdate2, tx3);
		awMapRef.update(gMapKey, gMapUpdate2, tx3);

		tx3.abortTransaction();
		tx3.close();

		antidoteClient.readOuterObjects(Arrays.asList(awMap));

		Assert.assertEquals(awMap.getCounterEntry("testCounter").getValue(), 5);
		Assert.assertEquals(awMap.getIntegerEntry("testInteger").getValue(), 8);
		Assert.assertTrue(awMap.getORSetEntry("testORSet").getValues().contains("Hi"));
		Assert.assertTrue(awMap.getORSetEntry("testORSet").getValues().contains("Hi2"));
		Assert.assertTrue(awMap.getRWSetEntry("testRWSet").getValues().contains("Hi"));
		Assert.assertTrue(awMap.getRWSetEntry("testRWSet").getValues().contains("Hi2"));
		Assert.assertNotEquals(awMap.getLWWRegisterEntry("testRegister").getValue(), "Hi2");
		Assert.assertNotEquals(awMap.getMVRegisterEntry("testMVRegister").getValueList(), "Hi2");
		Assert.assertEquals(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue(), 5);
		Assert.assertEquals(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue(), 5);

		AntidoteTransaction tx4 = antidoteClient.createStaticTransaction();

		awMapRef.update(counterKey,counterUpdate1,tx4);
		awMapRef.update(integerKey, intInc2, tx4);
		awMapRef.update(orSetKey, orSetRemove, tx4);
		awMapRef.update(orSetKey, orSetRemove2, tx4);
		awMapRef.update(rwSetKey, rwSetRemove, tx4);
		awMapRef.update(rwSetKey, rwSetRemove2, tx4);
		awMapRef.update(registerKey, registerUpdate2, tx4);
		awMapRef.update(mvRegisterKey, mvRegisterUpdate2, tx4);
		awMapRef.update(awMapKey, awMapUpdate2, tx4);
		awMapRef.update(gMapKey, gMapUpdate2, tx4);

		tx4.commitTransaction();
		tx4.close();

		antidoteClient.readOuterObjects(Arrays.asList(awMap));

		Assert.assertEquals(awMap.getCounterEntry("testCounter").getValue(), 10);
		Assert.assertEquals(awMap.getIntegerEntry("testInteger").getValue(), 10);
		Assert.assertThat(awMap.getORSetEntry("testORSet").getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi2")));
		Assert.assertThat(awMap.getRWSetEntry("testRWSet").getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi2")));
		Assert.assertEquals(awMap.getLWWRegisterEntry("testRegister").getValue(), "Hi2");
		Assert.assertThat(awMap.getMVRegisterEntry("testMVRegister").getValueList(), CoreMatchers.hasItem("Hi2"));
		Assert.assertEquals(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue(), 10);
		Assert.assertEquals(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue(), 10);
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

		int newValue1 = counter1new.getValue();
		int newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		antidoteTransaction.close();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1new.readDatabase(antidoteTransaction);
		counter2new.readDatabase(antidoteTransaction);
		newValue1 = counter1new.getValue();
		newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		antidoteTransaction.close();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
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

		antidoteTransaction.commitTransaction();

		AntidoteTransaction tx = antidoteClient.createTransaction();
		lowCounter1.increment(5, tx);
		lowCounter1.increment(5, tx);
		lowCounter2.increment(3, tx);
		lowCounter2.increment(3, tx);
		tx.commitTransaction();
		tx.close();
		
		antidoteTransaction = antidoteClient.createTransaction();
		AntidoteOuterCounter counter1new = lowCounter1.createAntidoteCounter(antidoteTransaction);
		AntidoteOuterCounter counter2new = lowCounter2.createAntidoteCounter(antidoteTransaction);
		antidoteTransaction.commitTransaction();

		int newValue1 = counter1new.getValue();
		int newValue2 = counter2new.getValue();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1new.readDatabase(antidoteTransaction);
		counter2new.readDatabase(antidoteTransaction);
		newValue1 = counter1new.getValue();
		newValue2 = counter2new.getValue();
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
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

		antidoteTransaction.commitTransaction();


		AntidoteTransaction tx3 = antidoteClient.createTransaction();
		counter1old.increment(5, tx3);
		counter1old.increment(5, tx3);
		counter2old.increment(3, tx3);
		counter2old.increment(3, tx3);
		tx3.commitTransaction();
		tx3.close();


		antidoteTransaction = antidoteClient.createTransaction();

		int newValue1 = counter1old.getValue();
		int newValue2 = counter2old.getValue();
		assert(newValue1 == oldValue1+10);
		assert(newValue2 == oldValue2+6);
		counter1old.readDatabase(antidoteTransaction);
		counter2old.readDatabase(antidoteTransaction);
		newValue1 = counter1old.getValue();
		newValue2 = counter2old.getValue();
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
	}

	@Test(timeout=10000)
	public void commitStaticTransaction() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter1 = new CounterRef("testCounter5", bucket, antidoteClient);
		CounterRef lowCounter2 = new CounterRef("testCounter3", bucket, antidoteClient);
		AntidoteOuterCounter counter1old = lowCounter1.createAntidoteCounter(antidoteTransaction);
		int oldValue1 = counter1old.getValue();
		AntidoteOuterCounter counter2old = lowCounter2.createAntidoteCounter(antidoteTransaction);
		int oldValue2 = counter2old.getValue();

		antidoteTransaction.commitTransaction();

		AntidoteTransaction tx = antidoteClient.createStaticTransaction();
		counter1old.increment(5, tx);
		counter1old.increment(5, tx);
		counter2old.increment(3, tx);
		counter2old.increment(3, tx);
		tx.commitTransaction();
		tx.close();


		antidoteTransaction = antidoteClient.createTransaction();

		int newValue1 = counter1old.getValue();
		int newValue2 = counter2old.getValue();
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
		antidoteTransaction = antidoteClient.createTransaction();
		counter1old.readDatabase(antidoteTransaction);
		counter2old.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(newValue1, oldValue1 + 10);
		Assert.assertEquals(newValue2, oldValue2 + 6);
	}

	@Test(timeout=10000)
	public void incBy2Test() {
		
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter = new CounterRef("testCounter5", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
		int oldValue = counter.getValue();
		counter.increment(antidoteTransaction);
		counter.increment(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = counter.getValue();
		Assert.assertEquals(newValue, oldValue + 2);
		antidoteTransaction = antidoteClient.createTransaction();
		counter.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		newValue = counter.getValue();
		Assert.assertEquals(newValue, oldValue + 2);
	}

	@Test(timeout=2000)
	public void decrementToZeroTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter testCounter = lowCounter.createAntidoteCounter(antidoteTransaction);
		testCounter.increment(0-testCounter.getValue(), antidoteTransaction);
		Assert.assertEquals(testCounter.getValue(), 0); //operation executed locally
		testCounter.readDatabase(antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(testCounter.getValue(), 0);
	}

	@Test(timeout=2000)
	public void incBy5Test(){
		antidoteTransaction = antidoteClient.createTransaction();

		CounterRef lowCounter = new CounterRef("testCounter", bucket, antidoteClient);
		AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
		int oldValue = counter.getValue();
		counter.increment(5, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		int newValue = counter.getValue();
		Assert.assertEquals(newValue, oldValue + 5);

	//	counter.readDatabase();
		newValue = counter.getValue();
		Assert.assertEquals(newValue, oldValue + 5);

	}

	@Test(timeout=2000)
	public void addElemTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		ORSetRef lowSet = new ORSetRef("testSet", bucket, antidoteClient);
		AntidoteOuterORSet testSet = lowSet.createAntidoteORSet(antidoteTransaction);
		testSet.addElement("element", antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertThat(testSet.getValues(), CoreMatchers.hasItem("element"));
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
		Assert.assertThat(testSet.getValues(), CoreMatchers.not(CoreMatchers.hasItem("Hi")));
		Assert.assertThat(testSet.getValues(), CoreMatchers.hasItem("Bye"));
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
		Assert.assertThat(testSet.getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Bye")));
		antidoteTransaction = antidoteClient.createTransaction();
		testSet.addElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertThat(testSet.getValues(), CoreMatchers.hasItems("Hi", "Bye"));
	}

	@Test(timeout=2000)
	public void updateRegTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		LWWRegisterRef lowReg = new LWWRegisterRef("testReg", bucket, antidoteClient);

        AntidoteOuterLWWRegister testReg = lowReg.createAntidoteLWWRegister(antidoteTransaction);
        testReg.setValue("hi", antidoteTransaction);
        testReg.setValue("bye", antidoteTransaction);
        antidoteTransaction.commitTransaction();
		Assert.assertEquals(testReg.getValue(), "bye");
		Assert.assertNotEquals(testReg.getValue(), "hi");
	}

	@Test(timeout=2000)
	public void updateMVRegTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		MVRegisterRef lowReg = new MVRegisterRef("testReg", bucket, antidoteClient);

		AntidoteOuterMVRegister testReg = lowReg.createAntidoteMVRegister(antidoteTransaction);
        testReg.setValue("hi", antidoteTransaction);
        testReg.setValue("bye", antidoteTransaction);
        antidoteTransaction.commitTransaction();
		Assert.assertThat(testReg.getValueList(), CoreMatchers.hasItem("bye"));
		Assert.assertThat(testReg.getValueList(), CoreMatchers.not(CoreMatchers.hasItem("hi")));
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
		Assert.assertEquals(oldValue + 1, newValue);
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
		Assert.assertEquals(oldValue - 5, newValue);
	}

	@Test(timeout=2000)
	public void setIntTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		IntegerRef lowInt = new IntegerRef("testInteger", bucket, antidoteClient);
		AntidoteOuterInteger integer = lowInt.createAntidoteInteger(antidoteTransaction);
		integer.setValue(42, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(integer.getValue(), 42);
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
		Assert.assertEquals(counterValue, 5);
		antidoteTransaction.commitTransaction();
		AntidoteInnerCounter counter = testMap.getCounterEntry(counterKey);
		counterValue = testMap.getCounterEntry(counterKey).getValue();
		Assert.assertEquals(counterValue, 5);
		counter = testMap.getCounterEntry(counterKey);
		antidoteTransaction = antidoteClient.createTransaction();
		counter.increment(5, antidoteTransaction);
		counter.increment(5, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertEquals(counterValue, 15);
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
		Assert.assertEquals(integerValue, 5);
		antidoteTransaction.commitTransaction();
		innerMap = testMap.getAWMapEntry(mapKey);
		AntidoteInnerInteger integer = innerMap.getIntegerEntry(integerKey);
		integerValue = integer.getValue();
		Assert.assertEquals(integerValue, 5);
		antidoteTransaction = antidoteClient.createTransaction();
		integer = innerMap.getIntegerEntry(integerKey);
		integer.increment(5, antidoteTransaction);
		integer.increment(5, antidoteTransaction);
		Assert.assertEquals(integer.getValue(), 15);
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
		Assert.assertEquals(registerValue, "yes");
		AntidoteInnerLWWRegister register = testMap.getLWWRegisterEntry(registerKey);
		antidoteTransaction.commitTransaction();
		registerValue = testMap.getLWWRegisterEntry(registerKey).getValue();
		Assert.assertEquals(registerValue, "yes");
		register = testMap.getLWWRegisterEntry(registerKey);

		antidoteTransaction = antidoteClient.createStaticTransaction();
		register.setValue("no", antidoteTransaction);
		register.setValue("maybe", antidoteTransaction);
		Assert.assertEquals(register.getValue(), "maybe");
		antidoteTransaction.commitTransaction();

	//	register.setValue("no");
	//	register.setValue("maybe");
		Assert.assertEquals(register.getValue(), "maybe"); // two local updates in a row
	//	register.synchronize();
		Assert.assertEquals(register.getValue(), "maybe"); // two updates sent to database at the same time, order is preserved
	//	register.setValue("");
	//	register.push();
	//	testMap.remove(registerKey, AntidoteType.LWWRegisterType);
	//	testMap.push(); // everything set to initial situation

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
		Assert.assertThat(registerValueList, CoreMatchers.hasItem("yes"));
		AntidoteInnerMVRegister register = testMap.getMVRegisterEntry(registerKey);
		antidoteTransaction.commitTransaction();
		registerValueList = testMap.getMVRegisterEntry(registerKey).getValueList();
		Assert.assertThat(registerValueList, CoreMatchers.hasItem("yes"));
		register = testMap.getMVRegisterEntry(registerKey);

		antidoteTransaction = antidoteClient.createTransaction();
		register.setValue("no", antidoteTransaction);
		register.setValueBS(ByteString.copyFromUtf8("maybe"), antidoteTransaction);
		Assert.assertThat(registerValueList, CoreMatchers.hasItem("maybe"));
		antidoteTransaction.commitTransaction();

	/*	register.setValue("no");
		register.setValueBS(ByteString.copyFromUtf8("maybe"));
		assert(register.getValueList().contains("maybe")); // two local updates in a row
		register.synchronize();
		assert(register.getValueList().contains("maybe")); // two updates sent to database at the same time, order is preserved
		register.setValue("");
		register.push();
		testMap.remove(registerKey, AntidoteType.MVRegisterType);
		testMap.push(); // everything set to initial situation*/

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
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes")); //local value is "yes"
		AntidoteInnerORSet set = testMap.getORSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getORSetEntry(setKey).getValues();
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes")); //update forwarded to database, then got a new state from database
		set = testMap.getORSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertThat(set.getValues(), CoreMatchers.not(CoreMatchers.hasItem("maybe")));
		Assert.assertThat(set.getValues(), CoreMatchers.hasItem(("no"))); // 3 local updates in a row
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
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes"));
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getRWSetEntry(setKey).getValues();
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes"));
		set = testMap.getRWSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertThat(set.getValues(), CoreMatchers.not(CoreMatchers.hasItem("maybe")));
		Assert.assertThat(set.getValues(), CoreMatchers.hasItem("no"));
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
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes"));
		AntidoteInnerRWSet set = testMap.getRWSetEntry(setKey);
		antidoteTransaction.commitTransaction();
		setValueList = testMap.getRWSetEntry(setKey).getValues();
		Assert.assertThat(setValueList, CoreMatchers.hasItem("yes"));
		set = testMap.getRWSetEntry(setKey);
		antidoteTransaction = antidoteClient.createTransaction();
		set.addElement("no", antidoteTransaction);
		List<String> elements = new ArrayList<>();
		elements.add("maybe");
		set.addElement(elements, antidoteTransaction);
		set.removeElement(elements, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		Assert.assertThat(set.getValues(), CoreMatchers.not(CoreMatchers.hasItem("maybe")));
		Assert.assertThat(set.getValues(), CoreMatchers.hasItem("no"));
	}

	@Test(timeout=10000)
	public void createRemoveTest() {
		antidoteTransaction = antidoteClient.createTransaction();
		AWMapRef lowMap = new AWMapRef("emptyMapBestMap", bucket, antidoteClient);


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

		testMap.update(awMapKey, awMapUpdate, antidoteTransaction);
		
		antidoteTransaction.commitTransaction();

	//	testMap.update(awMapKey, awMapUpdate);


	//	testMap.synchronize();
		AntidoteMapUpdate orSetRemove = AntidoteMapUpdate.createMapRemove(orSetKey, AntidoteType.ORSetType);
		AntidoteMapUpdate rwSetRemove = AntidoteMapUpdate.createMapRemove(rwSetKey, AntidoteType.RWSetType);
		AntidoteMapUpdate counterRemove = AntidoteMapUpdate.createMapRemove(counterKey, AntidoteType.CounterType);
		AntidoteMapUpdate integerRemove = AntidoteMapUpdate.createMapRemove(integerKey, AntidoteType.IntegerType);
		AntidoteMapUpdate registerRemove = AntidoteMapUpdate.createMapRemove(registerKey, AntidoteType.LWWRegisterType);
		AntidoteMapUpdate mvRegisterRemove = AntidoteMapUpdate.createMapRemove(mvRegisterKey, AntidoteType.MVRegisterType);
		AntidoteMapUpdate awMapRemove = AntidoteMapUpdate.createMapRemove(innerAWMapKey, AntidoteType.AWMapType);
		AntidoteMapUpdate gMapRemove = AntidoteMapUpdate.createMapRemove(gMapKey, AntidoteType.GMapType);


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
/*
		testMap.update(awMapKey, orSetRemove);
		testMap.update(awMapKey, rwSetRemove);
		testMap.update(awMapKey, counterRemove);
		testMap.update(awMapKey, integerRemove);
		testMap.update(awMapKey, registerRemove);
		testMap.update(awMapKey, mvRegisterRemove);
		testMap.update(awMapKey, awMapRemove);
		testMap.update(awMapKey, gMapRemove);


		testMap.synchronize();*/
		AntidoteInnerAWMap innerMap = testMap.getAWMapEntry(awMapKey);
		Assert.assertEquals(innerMap.getEntryList().size(), 0);
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

		Assert.assertEquals(counter.getValue(), 2);
		Assert.assertEquals(integer.getValue(), 8);
		Assert.assertEquals(register.getValue(),"Hi");
		Assert.assertThat(mvRegister.getValueList(), CoreMatchers.hasItem("Hi"));
		Assert.assertThat(orSet.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(orSet.getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi3")));
		Assert.assertThat(rwSet.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(rwSet.getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi3")));
		Assert.assertEquals(awMap.getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(awMap.getIntegerEntry("testInteger").getValue(), 5);
		Assert.assertThat(awMap.getORSetEntry("testORSet").getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(awMap.getRWSetEntry("testRWSet").getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertEquals(awMap.getLWWRegisterEntry("testRegister").getValue(), "Hi");
		Assert.assertThat(awMap.getMVRegisterEntry("testMVRegister").getValueList(), CoreMatchers.hasItem("Hi"));
		Assert.assertEquals(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(gMap.getCounterEntry("testCounter").getValue(), 1);

		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowAWMap.remove(counterKey, antidoteTransaction);
		lowLWWRegister.setBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		lowMVRegister.setBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		
		register.readDatabase(antidoteTransaction);
		mvRegister.readDatabase(antidoteTransaction);
		awMap.readDatabase(antidoteTransaction);
		
		Assert.assertEquals(register.getValue(), "Hi2");
		Assert.assertThat(mvRegister.getValueList(), CoreMatchers.hasItem("Hi2"));
		Assert.assertEquals(awMap.getCounterEntry("testCounter"), null);
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

		lowCounter.increment(2, antidoteTransaction);
		lowInteger.set(7, antidoteTransaction);
		lowInteger.increment(1, antidoteTransaction);
		
		lowORSet.add("Hi", antidoteTransaction);
		lowRWSet.add("Hi", antidoteTransaction);

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

		Assert.assertEquals(counter.getValue(), 4);
		Assert.assertEquals(integer.getValue(), 8);
		Assert.assertEquals(register.getValue(), "Hi");
		Assert.assertThat(mvRegister.getValueList(), CoreMatchers.hasItem("Hi"));
		Assert.assertThat(orSet.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(orSet.getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi3")));
		Assert.assertThat(rwSet.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(rwSet.getValues(), CoreMatchers.not(CoreMatchers.hasItems("Hi", "Hi3")));
		Assert.assertEquals(awMap.getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(awMap.getIntegerEntry("testInteger").getValue(), 5);
		Assert.assertThat(awMap.getORSetEntry("testORSet").getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(awMap.getRWSetEntry("testRWSet").getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertEquals(awMap.getLWWRegisterEntry("testRegister").getValue(), "Hi");
		Assert.assertThat(awMap.getMVRegisterEntry("testMVRegister").getValueList(), CoreMatchers.hasItem("Hi"));
		Assert.assertEquals(awMap.getAWMapEntry("testAWMap").getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(awMap.getGMapEntry("testGMap").getCounterEntry("testCounter").getValue(), 1);
		Assert.assertEquals(gMap.getCounterEntry("testCounter").getValue(), 1);
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

		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerGMap testGMap3 = testAWMap2.getGMapEntry("innerGMap");
		AntidoteInnerCounter counter1 = testAWMap.getCounterEntry("counterKey");
		AntidoteInnerCounter counter2 = testAWMap2.getCounterEntry("counterKey");
		AntidoteInnerCounter counter3 = testGMap3.getCounterEntry("counterKey");


		counter1.increment(1, antidoteTransaction);
		counter2.increment(2, antidoteTransaction);
		counter3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		Assert.assertEquals(counter1.getValue(), 6);
		Assert.assertEquals(counter2.getValue(), 7);
		Assert.assertEquals(counter3.getValue(), 8);

		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.CounterType, "counterKey"), counterUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerAWMap testAWMap3 = testGMap2.getAWMapEntry("innerAWMap");
		counter1 = testGMap.getCounterEntry("counterKey");
		counter2 = testGMap2.getCounterEntry("counterKey");
		counter3 = testAWMap3.getCounterEntry("counterKey");


		counter1.increment(1, antidoteTransaction);
		counter2.increment(2, antidoteTransaction);
		counter3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertEquals(counter1.getValue(), 6);
		Assert.assertEquals(counter2.getValue(), 7);
		Assert.assertEquals(counter3.getValue(), 8);
	}

	@Test(timeout=2000)
	public void integerTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate integerUpdate = AntidoteMapUpdate.createIntegerIncrement(5);
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);

		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerInteger integer1 = testAWMap.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer2 = testAWMap2.getIntegerEntry("integerKey");
		AntidoteInnerInteger integer3 = testAWMap3.getIntegerEntry("integerKey");


		integer1.increment(1, antidoteTransaction);
		integer2.increment(2, antidoteTransaction);
		integer3.increment(3, antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertEquals(integer1.getValue(), 6);
		Assert.assertEquals(integer2.getValue(), 7);
		Assert.assertEquals(integer3.getValue(), 8);
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("integerKey", integerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.IntegerType, "integerKey"), integerUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		integer1 = testGMap.getIntegerEntry("integerKey");
		integer2 = testGMap2.getIntegerEntry("integerKey");
		integer3 = testGMap3.getIntegerEntry("integerKey");


		integer1.increment(1, antidoteTransaction);
		integer2.increment(2, antidoteTransaction);
		integer3.increment(3, antidoteTransaction);

		antidoteTransaction.commitTransaction();

		Assert.assertEquals(integer1.getValue(), 6);
		Assert.assertEquals(integer2.getValue(), 7);
		Assert.assertEquals(integer3.getValue(), 8);
	}

	@Test(timeout=3000)
	public void orSetTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate orSetUpdate = AntidoteMapUpdate.createORSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);

		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerORSet orSet1 = testAWMap.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet2 = testAWMap2.getORSetEntry("orSetKey");
		AntidoteInnerORSet orSet3 = testAWMap3.getORSetEntry("orSetKey");


		orSet1.addElement("Hi2", antidoteTransaction);
		orSet2.addElement("Hi3", antidoteTransaction);
		orSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertThat(orSet1.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(orSet2.getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(orSet3.getValues(), CoreMatchers.hasItem("Hi4"));
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("orSetKey", orSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.ORSetType, "orSetKey"), orSetUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);

		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		orSet1 = testGMap.getORSetEntry("orSetKey");
		orSet2 = testGMap2.getORSetEntry("orSetKey");
		orSet3 = testGMap3.getORSetEntry("orSetKey");

		orSet1.addElement("Hi2", antidoteTransaction);
		orSet2.addElement("Hi3", antidoteTransaction);
		orSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertThat(orSet1.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(orSet2.getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(orSet3.getValues(), CoreMatchers.hasItem("Hi4"));
	}

	@Test(timeout=2000)
	public void rwSetTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate rwSetUpdate = AntidoteMapUpdate.createRWSetAdd("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);

		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);
		
		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerRWSet rwSet1 = testAWMap.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet2 = testAWMap2.getRWSetEntry("rwSetKey");
		AntidoteInnerRWSet rwSet3 = testAWMap3.getRWSetEntry("rwSetKey");


		rwSet1.addElement("Hi2", antidoteTransaction);
		rwSet2.addElement("Hi3", antidoteTransaction);
		rwSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertThat(rwSet1.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(rwSet2.getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(rwSet3.getValues(), CoreMatchers.hasItem("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("rwSetKey", rwSetUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);

		
		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.RWSetType, "rwSetKey"), rwSetUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		rwSet1 = testGMap.getRWSetEntry("rwSetKey");
		rwSet2 = testGMap2.getRWSetEntry("rwSetKey");
		rwSet3 = testGMap3.getRWSetEntry("rwSetKey");


		rwSet1.addElement("Hi2", antidoteTransaction);
		rwSet2.addElement("Hi3", antidoteTransaction);
		rwSet3.addElement("Hi4", antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertThat(rwSet1.getValues(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(rwSet2.getValues(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(rwSet3.getValues(), CoreMatchers.hasItem("Hi4"));
	}

	@Test(timeout=2000)
	public void registerTest2() {
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate registerUpdate = AntidoteMapUpdate.createRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);

		lowAWMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerLWWRegister register1 = testAWMap.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register2 = testAWMap2.getLWWRegisterEntry("registerKey");
		AntidoteInnerLWWRegister register3 = testAWMap3.getLWWRegisterEntry("registerKey");

		register1.setValueBS(ByteString.copyFromUtf8("Hi2"), antidoteTransaction);
		register2.setValueBS(ByteString.copyFromUtf8("Hi3"), antidoteTransaction);
		register3.setValueBS(ByteString.copyFromUtf8("Hi4"), antidoteTransaction);
		antidoteTransaction.commitTransaction();
		
		Assert.assertEquals(register1.getValue(), "Hi2");
		Assert.assertEquals(register2.getValue(), "Hi3");
		Assert.assertEquals(register3.getValue(), "Hi4");
		
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("registerKey", registerUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.LWWRegisterType, "registerKey"), registerUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
		
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		register1 = testGMap.getLWWRegisterEntry("registerKey");
		register2 = testGMap2.getLWWRegisterEntry("registerKey");
		register3 = testGMap3.getLWWRegisterEntry("registerKey");


		register1.setValue("Hi2", antidoteTransaction);
		register2.setValue("Hi3", antidoteTransaction);
		register3.setValue("Hi4", antidoteTransaction);

		antidoteTransaction.commitTransaction();

		Assert.assertEquals(register1.getValue(), "Hi2");
		Assert.assertEquals(register2.getValue(), "Hi3");
		Assert.assertEquals(register3.getValue(), "Hi4");
	}

	@Test(timeout=2000)
	public void mvRegisterTest2(){
		antidoteTransaction = antidoteClient.createTransaction();
		
		AntidoteMapUpdate mvRegisterUpdate = AntidoteMapUpdate.createMVRegisterSet("Hi");
		AntidoteMapUpdate awMapUpdate = AntidoteMapUpdate.createAWMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate awMapUpdate2 = AntidoteMapUpdate.createAWMapUpdate("innerAWMap", awMapUpdate);

		AWMapRef lowAWMap = new AWMapRef("outerAWMap", bucket, antidoteClient);
		GMapRef lowGMap = new GMapRef("outerGMap", bucket, antidoteClient);
		
		lowAWMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate, antidoteTransaction);
		lowAWMap.update(new AntidoteMapKey(AntidoteType.AWMapType, "innerAWMap"), awMapUpdate2, antidoteTransaction);

		AntidoteOuterAWMap testAWMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);

		AntidoteInnerAWMap testAWMap2 = testAWMap.getAWMapEntry("innerAWMap");
		AntidoteInnerAWMap testAWMap3 = testAWMap2.getAWMapEntry("innerAWMap");
		AntidoteInnerMVRegister mvRegister1 = testAWMap.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister2 = testAWMap2.getMVRegisterEntry("mvRegisterKey");
		AntidoteInnerMVRegister mvRegister3 = testAWMap3.getMVRegisterEntry("mvRegisterKey");

		mvRegister1.setValue("Hi2", antidoteTransaction);
		mvRegister2.setValueBS(ByteString.copyFromUtf8("Hi3"), antidoteTransaction);
		mvRegister3.setValueBS(ByteString.copyFromUtf8("Hi4"), antidoteTransaction);
		antidoteTransaction.commitTransaction();

		Assert.assertThat(mvRegister1.getValueList(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(mvRegister2.getValueList(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(mvRegister3.getValueList(), CoreMatchers.hasItem("Hi4"));

		AntidoteMapUpdate gMapUpdate = AntidoteMapUpdate.createGMapUpdate("mvRegisterKey", mvRegisterUpdate);
		AntidoteMapUpdate gMapUpdate2 = AntidoteMapUpdate.createGMapUpdate("innerGMap", gMapUpdate);

		antidoteTransaction = antidoteClient.createTransaction();
		
		lowGMap.update(new AntidoteMapKey(AntidoteType.MVRegisterType, "mvRegisterKey"), mvRegisterUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate, antidoteTransaction);
		lowGMap.update(new AntidoteMapKey(AntidoteType.GMapType, "innerGMap"), gMapUpdate2, antidoteTransaction);
				
		AntidoteOuterGMap testGMap = lowGMap.createAntidoteGMap(antidoteTransaction);

		AntidoteInnerGMap testGMap2 = testGMap.getGMapEntry("innerGMap");
		AntidoteInnerGMap testGMap3 = testGMap2.getGMapEntry("innerGMap");
		mvRegister1 = testGMap.getMVRegisterEntry("mvRegisterKey");
		mvRegister2 = testGMap2.getMVRegisterEntry("mvRegisterKey");
		mvRegister3 = testGMap3.getMVRegisterEntry("mvRegisterKey");

		mvRegister1.setValue("Hi2", antidoteTransaction);
		mvRegister2.setValue("Hi3", antidoteTransaction);
		mvRegister3.setValue("Hi4", antidoteTransaction);

		antidoteTransaction.commitTransaction();

		Assert.assertThat(mvRegister1.getValueList(), CoreMatchers.hasItem("Hi2"));
		Assert.assertThat(mvRegister2.getValueList(), CoreMatchers.hasItem("Hi3"));
		Assert.assertThat(mvRegister3.getValueList(), CoreMatchers.hasItem("Hi4"));
	}
}