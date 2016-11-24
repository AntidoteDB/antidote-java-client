import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.*;
public class AntidoteClientExample {
    public static void main(String[] args) {
        AntidoteClient antidoteClient = new AntidoteClient("192.168.99.100", 8087);
        antidoteClient.updateCounter("testCounter", "testBucket");
        int i = antidoteClient.readCounter("testCounter", "testBucket");
        System.out.println(i);
        antidoteClient.addSet("testSet", "testBucket", "half life 3 confirmed");
        List<String> testList = new ArrayList<String>();
        testList = antidoteClient.readSet("testSet", "testBucket");
        for (String e: testList){
        	System.out.println(e);
        }
        antidoteClient.updateRegister("testReg", "testBucket", "hi");
        antidoteClient.updateRegister("testReg", "testBucket", "bye");
        String s = antidoteClient.readRegister("testReg", "testBucket");
        System.out.println(s);
        
        antidoteClient.updateMVRegister("test2Reg", "testBucket", "hello");
        antidoteClient.updateMVRegister("test2Reg", "testBucket", "goodbye");
        s = antidoteClient.readMVRegister("test2Reg", "testBucket");
        System.out.println(s);
        
        antidoteClient.setInteger("testInt", "testBucket", 3);
        antidoteClient.incrementInteger("testInt", "testBucket", 2);
        i = antidoteClient.readInteger("testInt", "testBucket");
        System.out.println(i);
    }
}
