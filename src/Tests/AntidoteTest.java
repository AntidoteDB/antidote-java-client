package Tests;
import AntidoteClient.*;
import java.util.*;
import java.util.ArrayList;

import org.junit.Test;

import com.basho.riak.protobuf.AntidotePB.ApbGetCounterResp;
import com.basho.riak.protobuf.AntidotePB.ApbGetIntegerResp;
import com.basho.riak.protobuf.AntidotePB.ApbGetMVRegResp;
import com.basho.riak.protobuf.AntidotePB.ApbGetMapResp;
import com.basho.riak.protobuf.AntidotePB.ApbGetRegResp;
import com.basho.riak.protobuf.AntidotePB.ApbGetSetResp;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

public class AntidoteTest{
	AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
	
	@Test
	public void addElemTest() {
		antidoteClient.addSetElement("testSet", "testBucket", "element");
		ApbGetSetResp testSet = antidoteClient.readSet("testSet", "testBucket");
		assert(testSet.getValueList().contains(ByteString.copyFromUtf8("element")));
	}
	
	@Test
	public void addElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		antidoteClient.addSetElement("testSet", "testBucket", elements);
		ApbGetSetResp testSet = antidoteClient.readSet("testSet", "testBucket");
		assert(testSet.getValueList().contains(ByteString.copyFromUtf8("Hi")));
		assert(testSet.getValueList().contains(ByteString.copyFromUtf8("Bye")));
	}
	
	@Test
	public void remElemsTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		antidoteClient.addSetElement("testSet", "testBucket", elements);
		antidoteClient.removeSetElement("testSet", "testBucket", elements);
		ApbGetSetResp testSet = antidoteClient.readSet("testSet", "testBucket");
		assert(! testSet.getValueList().contains(ByteString.copyFromUtf8("Hi")));
		assert(! testSet.getValueList().contains(ByteString.copyFromUtf8("Bye")));
	}
	
	@Test
	public void remElemTest() {
		List<String> elements = new ArrayList<String>();
		elements.add("Hi");
		elements.add("Bye");
		antidoteClient.addSetElement("testSet", "testBucket", elements);
		antidoteClient.removeSetElement("testSet", "testBucket", "Hi");
		ApbGetSetResp testSet = antidoteClient.readSet("testSet", "testBucket");
		assert(! testSet.getValueList().contains(ByteString.copyFromUtf8("Hi")));
		assert(testSet.getValueList().contains(ByteString.copyFromUtf8("Bye")));
	}
	
	@Test
	public void decrementTest() {
		ApbGetCounterResp testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		antidoteClient.updateCounter("testCounter", "testBucket", 0-testCounter.getValue());
		testCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(testCounter.getValue() == 0);
	}
	
	@Test
	public void incBy1Test() {					
		ApbGetCounterResp oldCounter = antidoteClient.readCounter("testCounter", "testBucket");
		antidoteClient.updateCounter("testCounter", "testBucket");
        ApbGetCounterResp newCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(oldCounter.getValue()+1 == newCounter.getValue());		
	}
	
	@Test
	public void incBy5Test(){					
		ApbGetCounterResp oldCounter = antidoteClient.readCounter("testCounter", "testBucket");
		antidoteClient.updateCounter("testCounter", "testBucket", 5);
        ApbGetCounterResp newCounter = antidoteClient.readCounter("testCounter", "testBucket");
		assert(oldCounter.getValue()+5 == newCounter.getValue());		
	}
	
	@Test
	public void notInitializedTest(){
		ApbGetCounterResp notInitCounter = antidoteClient.readCounter("someCounter", "someBucket");
		assert(notInitCounter.getValue() == 0);		
	}
	
	@Test
	public void updateRegTest() {
		antidoteClient.updateRegister("testReg", "testBucket", "hi");
        antidoteClient.updateRegister("testReg", "testBucket", "bye");
        ApbGetRegResp testReg = antidoteClient.readRegister("testReg", "testBucket");
        assert(testReg.getValue().toStringUtf8().equals("bye"));
        assert(! testReg.getValue().toStringUtf8().equals("hi"));
	}
	
	@Test
	public void updateMVRegTest() {
		antidoteClient.updateMVRegister("testMVReg", "testBucket", "hi");
        antidoteClient.updateMVRegister("testMVReg", "testBucket", "bye");
        ApbGetMVRegResp testReg = antidoteClient.readMVRegister("testMVReg", "testBucket");
        assert(testReg.getValuesList().contains(ByteString.copyFromUtf8("bye")));
        assert(! testReg.getValuesList().contains(ByteString.copyFromUtf8("hi")));
	}
	
	@Test
	public void notInitializedSet(){
		ApbGetMVRegResp testReg = antidoteClient.readMVRegister("someReg", "someBucket");
        assert(testReg.getValuesList().isEmpty());
	}
	
	@Test
	public void incIntBy1Test() {
		ApbGetIntegerResp oldInteger = antidoteClient.readInteger("testInteger", "testBucket");
		antidoteClient.incrementInteger("testInteger", "testBucket");
		ApbGetIntegerResp newInteger = antidoteClient.readInteger("testInteger", "testBucket");
		assert(oldInteger.getValue() + 1 == newInteger.getValue());
	}
	
	@Test
	public void decBy5Test() {
		ApbGetIntegerResp oldInteger = antidoteClient.readInteger("testInteger", "testBucket");
		antidoteClient.incrementInteger("testInteger", "testBucket", -5);
		ApbGetIntegerResp newInteger = antidoteClient.readInteger("testInteger", "testBucket");
		assert(oldInteger.getValue() - 5 == newInteger.getValue());
	}
	
	@Test
	public void setIntTest() {					
		antidoteClient.setInteger("testInteger", "testBucket", 42);
		ApbGetIntegerResp newInteger = antidoteClient.readInteger("testInteger", "testBucket");
		assert(newInteger.getValue() == 42);	
	}
	
	@Test
	public void counterTest() {
        ApbUpdateOperation counterInc = antidoteClient.createCounterIncrementOperation(3);
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.COUNTER);
        ApbMapKey keyCounter = keyBuilder.build();
        ApbGetCounterResp oldCounter = antidoteClient.mapGetCounter("testMap", "testBucket", "key");
        antidoteClient.updateMap("testMap", "testBucket", keyCounter, counterInc);
        ApbGetCounterResp newCounter = antidoteClient.mapGetCounter("testMap", "testBucket", "key");
        assert(newCounter.getValue() == oldCounter.getValue()+3);
	}
		
	@Test
	public void setTest() {
		ApbUpdateOperation setAdd = antidoteClient.createSetAddElementOperation("bye");
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.ORSET);
        ApbMapKey keySet = keyBuilder.build();
        antidoteClient.updateMap("testMap", "testBucket", keySet, setAdd);
        ApbGetSetResp newSet = antidoteClient.mapGetSet("testMap", "testBucket", "key");
        assert(newSet.getValueList().contains(ByteString.copyFromUtf8("bye")));
	}
	
	@Test
	public void intTest() {
		ApbUpdateOperation integerInc = antidoteClient.createIntegerIncrementOperation(3);
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.INTEGER);
        ApbMapKey keyInteger = keyBuilder.build();
        ApbGetIntegerResp oldInt = antidoteClient.mapGetInteger("testMap", "testBucket", "key");
        antidoteClient.updateMap("testMap", "testBucket", keyInteger, integerInc);
        ApbGetIntegerResp newInt = antidoteClient.mapGetInteger("testMap", "testBucket", "key");
        assert(oldInt.getValue()+3 == newInt.getValue());
        
	}
	
	@Test
	public void regTest() {
		ApbUpdateOperation regSet = antidoteClient.createRegisterSetOperation("bye");        
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.LWWREG);
        ApbMapKey keyReg = keyBuilder.build();
        antidoteClient.updateMap("testMap", "testBucket", keyReg, regSet);
        ApbGetRegResp newReg = antidoteClient.mapGetRegister("testMap", "testBucket", "key");
        assert(newReg.getValue().toStringUtf8().equals("bye"));
	}
	
	@Test
	public void mvRegTest() {
		ApbUpdateOperation regSet = antidoteClient.createRegisterSetOperation("bye");        
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.MVREG);
        ApbMapKey keyReg = keyBuilder.build();
        antidoteClient.updateMap("testMap", "testBucket", keyReg, regSet);
        ApbGetMVRegResp newReg = antidoteClient.mapGetMVRegister("testMap", "testBucket", "key");
        assert(newReg.getValuesList().contains(ByteString.copyFromUtf8("bye")));

	}
	
	@Test
	public void mapTest() {
		ApbUpdateOperation intSet = antidoteClient.createIntegerSetOperation(3);
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.INTEGER);
        ApbMapKey keyInteger = keyBuilder.build();		
		ApbUpdateOperation mapUpdate = antidoteClient.createMapUpdateOperation(keyInteger, intSet);    
        keyBuilder.setType(CRDT_type.AWMAP);
        ApbMapKey keyMap = keyBuilder.build();
        antidoteClient.updateMap("testMap", "testBucket", keyMap, mapUpdate);
        ApbGetMapResp newMap = antidoteClient.mapGetMap("testMap", "testBucket", "key");
        ApbGetIntegerResp newInteger = antidoteClient.nestedMapGetInteger(newMap, "key");
        assert(newInteger.getValue() == 3);
	}
}
