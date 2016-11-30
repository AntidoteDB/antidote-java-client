import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.ByteString;
public class AntidoteClientExample {
    public static void main(String[] args) {
        
		AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
		
        antidoteClient.updateCounter("testCounter", "testBucket");
        ApbGetCounterResp testCounter = antidoteClient.readCounter("testCounter", "testBucket");
        System.out.println(testCounter);
        
        antidoteClient.addSet("testSet", "testBucket", "half life 3 confirmed");
        ApbGetSetResp testSet = antidoteClient.readSet("testSet", "testBucket");
        System.out.println(testSet);
        
        antidoteClient.updateRegister("testReg", "testBucket", "hi");
        antidoteClient.updateRegister("testReg", "testBucket", "bye");
        ApbGetRegResp testReg = antidoteClient.readRegister("testReg", "testBucket");
        System.out.println(testReg);
        
        antidoteClient.updateMVRegister("test2Reg", "testBucket", "hello");
        antidoteClient.updateMVRegister("test2Reg", "testBucket", "goodbye");
        ApbGetMVRegResp testMVReg = antidoteClient.readMVRegister("test2Reg", "testBucket");
        System.out.println(testMVReg);
        
        antidoteClient.setInteger("testInt", "testBucket", 3);
        antidoteClient.incrementInteger("testInt", "testBucket", 2);
        ApbGetIntegerResp integer = antidoteClient.readInteger("testInt", "testBucket");
        System.out.println(integer);

        ApbUpdateOperation counterInc = antidoteClient.createCounterIncrement(3);
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.COUNTER);
        ApbMapKey keyCounter = keyBuilder.build();
        
        ApbUpdateOperation integerInc = antidoteClient.createIntegerIncrement(3);
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.INTEGER);
        ApbMapKey keyInteger = keyBuilder.build();
        
        ApbUpdateOperation setAdd = antidoteClient.createSetAdd("bye");
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.ORSET);
        ApbMapKey keySet = keyBuilder.build();
        
        ApbUpdateOperation regSet = antidoteClient.createRegSet("bye");
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.LWWREG);
        ApbMapKey keyReg = keyBuilder.build();
        
        ApbUpdateOperation mvRegSet = antidoteClient.createRegSet("bye");
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.MVREG);
        ApbMapKey keyMVReg = keyBuilder.build();
        
        ApbUpdateOperation mapUpdate = antidoteClient.createMapUpdate(keyCounter, counterInc);
        keyBuilder.setKey(ByteString.copyFromUtf8("key"));
        keyBuilder.setType(CRDT_type.AWMAP);
        ApbMapKey keyMap = keyBuilder.build();
        
        ApbUpdateOperation mapUpdate2 = antidoteClient.createMapUpdate(keyMap, mapUpdate);
        
        ApbGetCounterResp counter = antidoteClient.mapGetCounter("testMap2", "testBucket", "key");
        System.out.println(counter);
        
        
        antidoteClient.updateMap("testMap2", "testBucket", keyInteger, counterInc);
        antidoteClient.updateMap("testMap2", "testBucket", keyCounter, integerInc);
        antidoteClient.updateMap("testMap2", "testBucket", keySet, setAdd);
        antidoteClient.updateMap("testMap2", "testBucket", keyReg, regSet);
        antidoteClient.updateMap("testMap2", "testBucket", keyMVReg, mvRegSet);
        antidoteClient.updateMap("testMap2", "testBucket", keyMap, mapUpdate);
        antidoteClient.updateMap("testMap2", "testBucket", keyMap, mapUpdate2);
       // antidoteClient.removeMap("testMap2", "testBucket", keyCounter);
        ApbGetMapResp testMap = antidoteClient.readMap("testMap2", "testBucket");
        System.out.println(testMap);
        System.out.println(antidoteClient.mapGetCounter("testMap2", "testBucket", "key"));
        System.out.println(antidoteClient.mapGetSet("testMap2", "testBucket", "key"));
        System.out.println(antidoteClient.mapGetReg("testMap2", "testBucket", "key"));
        System.out.println(antidoteClient.mapGetMVReg("testMap2", "testBucket", "key"));
        System.out.println(antidoteClient.mapGetInteger("testMap2", "testBucket", "key"));
        System.out.println(antidoteClient.mapGetMap("testMap2", "testBucket", "key"));
    }
}
