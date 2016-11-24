import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.*;
import java.io.*;
import java.net.Socket;
import static java.lang.Math.toIntExact;

public class AntidoteClient {
    private Socket socket;
    private String host;
    private int port;

    public AntidoteClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    // if no parameter is given, increment by 1
    public void updateCounter(String name, String bucket){
    	updateCounter(name, bucket, 1);
    }
    
    public void updateCounter(String name, String bucket, Integer inc) {

        ApbStaticUpdateObjects.Builder counterUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder counterObject = ApbBoundObject.newBuilder(); // The object in the message
        counterObject.setKey(ByteString.copyFromUtf8(name));
        counterObject.setType(CRDT_type.COUNTER);
        counterObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(counterObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        counterUpdateMessage.setTransaction(writeTransaction);
        counterUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects counterUpdateMessageObject = counterUpdateMessage.build();

        int messageLength = counterUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            counterUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public int readCounter(String name, String bucket) {

        ApbBoundObject.Builder counterObject = ApbBoundObject.newBuilder(); // The object in the message
        counterObject.setKey(ByteString.copyFromUtf8(name));
        counterObject.setType(CRDT_type.COUNTER);
        counterObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(counterObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);
            ApbGetCounterResp counter = readResponse.getObjects().getObjects(0).getCounter();
            int val = counter.getValue();
            return val;

        } catch (Exception e) {
            System.out.println(e);
            return -1;
        }  
    }
    
    //assumption: when multiple elements are removed, the parameter is a list
    public void removeSet(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllRems(elementsByteString);
        updateSetHelper(name, bucket, setUpdateInstruction);
    }
    
    public void addSet(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllAdds(elementsByteString);
        updateSetHelper(name, bucket, setUpdateInstruction);
    }
    
    public void removeSet(String name, String bucket, String element){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addRems(ByteString.copyFromUtf8(element));
        updateSetHelper(name, bucket, setUpdateInstruction);
    }
    
    public void addSet(String name, String bucket, String element){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAdds(ByteString.copyFromUtf8(element)); 
        updateSetHelper(name, bucket, setUpdateInstruction);
    }
    
    public void updateSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction){
        ApbStaticUpdateObjects.Builder setUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder setObject = ApbBoundObject.newBuilder(); // The object in the message
        setObject.setKey(ByteString.copyFromUtf8(name));
        setObject.setType(CRDT_type.ORSET);
        setObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(setObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        setUpdateMessage.setTransaction(writeTransaction);
        setUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects setUpdateMessageObject = setUpdateMessage.build();

        int messageLength = setUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            setUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public List<String> readSet(String name, String bucket) {

        ApbBoundObject.Builder setObject = ApbBoundObject.newBuilder(); // The object in the message
        setObject.setKey(ByteString.copyFromUtf8(name));
        setObject.setType(CRDT_type.ORSET);
        setObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(setObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);
            System.out.println(readResponse);
            ApbGetSetResp set = readResponse.getObjects().getObjects(0).getSet();
            List<ByteString> valList = set.getValueList();
            List<String> valListString = new ArrayList<String>();      
            
            for (ByteString e : valList){
            	valListString.add(e.toStringUtf8());
            }
            socket.close();
            return valListString;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }       
    }
    
    public void updateRegister(String name, String bucket, String value){

        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.LWWREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);

    }
    
    public String readRegister(String name, String bucket) {

        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.LWWREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(regObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);
            
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);
            ApbGetRegResp reg = readResponse.getObjects().getObjects(0).getReg();
            ByteString val = reg.getValue();
            String valString = val.toStringUtf8();
            socket.close();
            return valString;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }   

    }
    
    public void updateMVRegister(String name, String bucket, String value){
    	
        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.MVREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);
    }
    
    public String readMVRegister(String name, String bucket) {
    	
    	ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.MVREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(regObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);
            
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);
            ApbGetMVRegResp reg = readResponse.getObjects().getObjects(0).getMvreg();           
            ByteString val = reg.getValues(0); // don't really know how to do this properly, TODO
            String valString = val.toStringUtf8();
            socket.close();
            return valString;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }        
    }
    
    public void updateRegisterHelper(String name, String bucket, String value, ApbBoundObject.Builder regObject){
        ApbStaticUpdateObjects.Builder regUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(ByteString.copyFromUtf8(value));

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(regObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        regUpdateMessage.setTransaction(writeTransaction);
        regUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects regUpdateMessageObject = regUpdateMessage.build();

        int messageLength = regUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            regUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public void incrementInteger(String name, String bucket, Integer inc) {

        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(inc); // Set increment
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    public void setInteger(String name, String bucket, Integer number) {

        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(number); //Set the integer to this value
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    public void updateIntegerHelper(String name, String bucket, ApbIntegerUpdate.Builder intUpdateInstruction) {

        ApbStaticUpdateObjects.Builder intUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder intObject = ApbBoundObject.newBuilder(); // The object in the message
        intObject.setKey(ByteString.copyFromUtf8(name));
        intObject.setType(CRDT_type.INTEGER);
        intObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(intObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        intUpdateMessage.setTransaction(writeTransaction);
        intUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects intUpdateMessageObject = intUpdateMessage.build();

        int messageLength = intUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            intUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public int readInteger(String name, String bucket) {

        ApbBoundObject.Builder intObject = ApbBoundObject.newBuilder(); // The object in the message
        intObject.setKey(ByteString.copyFromUtf8(name));
        intObject.setType(CRDT_type.INTEGER);
        intObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(intObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);
            ApbGetIntegerResp number = readResponse.getObjects().getObjects(0).getInt();
            int val = toIntExact(number.getValue());
            return val;

        } catch (Exception e) {
            System.out.println(e);
            return -1;
        }       
    }
    
    
   	public void updateMap(String name, String bucket, ApbMapKey key, List<ApbUpdateOperation> updates) {

        ApbStaticUpdateObjects.Builder mapUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder mapObject = ApbBoundObject.newBuilder(); // The object in the message
        mapObject.setKey(ByteString.copyFromUtf8(name));
        mapObject.setType(CRDT_type.AWMAP);
        mapObject.setBucket(ByteString.copyFromUtf8(bucket));
        
        ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        
        int i=0;
        for (ApbUpdateOperation update : updates){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(key);
        	ApbMapNestedUpdate mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(i, mapNestedUpdate);
        	i++;
        }

        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        mapUpdateInstruction.addAllUpdates(mapNestedUpdateList);
            
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(mapObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        mapUpdateMessage.setTransaction(writeTransaction);
        mapUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects mapUpdateMessageObject = mapUpdateMessage.build();

        int messageLength = mapUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            mapUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        } 	
        
    }
   	
   	public void removeMap(String name, String bucket, ApbMapKey key){
    	List<ApbMapKey> keys = new ArrayList<ApbMapKey>();
    	keys.add(0, key);
    	removeMap(name, bucket, keys);
    }
   	
   	public void removeMap(String name, String bucket, List<ApbMapKey> keys) {

        ApbStaticUpdateObjects.Builder mapUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder mapObject = ApbBoundObject.newBuilder(); // The object in the message
        mapObject.setKey(ByteString.copyFromUtf8(name));
        mapObject.setType(CRDT_type.AWMAP);
        mapObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        mapUpdateInstruction.addAllRemovedKeys(keys);
        
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(mapObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        mapUpdateMessage.setTransaction(writeTransaction);
        mapUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects mapUpdateMessageObject = mapUpdateMessage.build();

        int messageLength = mapUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            mapUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}