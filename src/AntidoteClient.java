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

    public ApbGetCounterResp readCounter(String name, String bucket) {

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
            socket.close();
            return counter;

        } catch (Exception e) {
            System.out.println(e);
            return null;
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
    
    public ApbGetSetResp readSet(String name, String bucket) {

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
            ApbGetSetResp set = readResponse.getObjects().getObjects(0).getSet();
            socket.close();
            return set;

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
    
    public ApbGetRegResp readRegister(String name, String bucket) {

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
            socket.close();
            return reg;

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
    
    public ApbGetMVRegResp readMVRegister(String name, String bucket) {
    	
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
            socket.close();
            return reg;

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
    
    public ApbGetIntegerResp readInteger(String name, String bucket) {

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
            socket.close();
            return number;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }       
    }
    
   
    public void updateMap(String name, String bucket, String key, CRDT_type type, ApbUpdateOperation update) {
    	List<ApbUpdateOperation> updateList = new ArrayList<ApbUpdateOperation>();
    	updateList.add(update);
    	updateMap(name, bucket, key, type, updateList);
    }
    
    public void updateMap(String name, String bucket, String key, CRDT_type type, List<ApbUpdateOperation> updates){
        ApbMapKey.Builder mapKeyBuilder = ApbMapKey.newBuilder();
        mapKeyBuilder.setKey(ByteString.copyFromUtf8(key));
        mapKeyBuilder.setType(type);
        ApbMapKey mapKey = mapKeyBuilder.build();
        updateMap(name, bucket, mapKey, updates);
    }
    
    public void updateMap(String name, String bucket, ApbMapKey key, ApbUpdateOperation update){
    	List<ApbUpdateOperation> updateList = new ArrayList<ApbUpdateOperation>();
    	updateList.add(update);
    	updateMap(name, bucket, key, updateList);
    }
    
   	public void updateMap(String name, String bucket, ApbMapKey mapKey, List<ApbUpdateOperation> updates) {

        ApbStaticUpdateObjects.Builder mapUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder mapObject = ApbBoundObject.newBuilder(); // The object in the message
        mapObject.setKey(ByteString.copyFromUtf8(name));
        mapObject.setType(CRDT_type.AWMAP);
        mapObject.setBucket(ByteString.copyFromUtf8(bucket));
        
        ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        
        int i=0;
        for (ApbUpdateOperation update : updates){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(mapKey);
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
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
   	
   	public void removeMap(String name, String bucket, ApbMapKey key) {
    	List<ApbMapKey> keyList = new ArrayList<ApbMapKey>();
    	keyList.add(key);
    	removeMap(name, bucket, keyList);
    }
    
   	public void removeMap(String name, String bucket, List<ApbMapKey> keys) {

        ApbStaticUpdateObjects.Builder mapUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder mapObject = ApbBoundObject.newBuilder(); // The object in the message
        mapObject.setKey(ByteString.copyFromUtf8(name));
        mapObject.setType(CRDT_type.AWMAP);
        mapObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        for (ApbMapKey key : keys){
        	mapUpdateInstruction.addRemovedKeys(key);
        }
            
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
   	
   	public ApbGetMapResp readMap(String name, String bucket) {

        ApbBoundObject.Builder intObject = ApbBoundObject.newBuilder(); // The object in the message
        intObject.setKey(ByteString.copyFromUtf8(name));
        intObject.setType(CRDT_type.AWMAP);
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
            ApbGetMapResp map = readResponse.getObjects().getObjects(0).getMap();
            return map;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }       
    }
   	
   	//methods to create ApbUpdateOperations needed for map updates
   	
    public ApbUpdateOperation createCounterIncrement(){
    	return createCounterIncrement(1);
    };
   	
   	public ApbUpdateOperation createCounterIncrement(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbCounterUpdate.Builder upBuilder = ApbCounterUpdate.newBuilder();
    	upBuilder.setInc(value);
    	ApbCounterUpdate up = upBuilder.build();
    	opBuilder.setCounterop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createIntegerIncrement(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setInc(value);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createIntegerSet(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setSet(value);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createRegSet(String value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbRegUpdate.Builder upBuilder = ApbRegUpdate.newBuilder();
    	upBuilder.setValue(ByteString.copyFromUtf8(value));
    	ApbRegUpdate up = upBuilder.build();
    	opBuilder.setRegop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createMapUpdate(String key, CRDT_type type, ApbUpdateOperation update){
        List<ApbUpdateOperation> updates = new ArrayList<ApbUpdateOperation>();
        updates.add(update);
        return createMapUpdate(key, type, updates);
    };
    
    public ApbUpdateOperation createMapUpdate(ApbMapKey key, ApbUpdateOperation update){
        List<ApbUpdateOperation> updates = new ArrayList<ApbUpdateOperation>();
        updates.add(update);
        return createMapUpdate(key, updates);
    };
    
    public ApbUpdateOperation createMapUpdate(String key, CRDT_type type, List <ApbUpdateOperation> updates){
        ApbMapKey.Builder mapKeyBuilder = ApbMapKey.newBuilder();
        mapKeyBuilder.setKey(ByteString.copyFromUtf8(key));
        mapKeyBuilder.setType(type);
        ApbMapKey mapKey = mapKeyBuilder.build();
        return createMapUpdate(mapKey, updates);
    };
    
    public ApbUpdateOperation createMapUpdate(ApbMapKey key, List <ApbUpdateOperation> updates){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	
    	ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder();
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        
        int i=0;
        for (ApbUpdateOperation update : updates){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(key);
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(i, mapNestedUpdate);
        	i++;
        }
    	upBuilder.addAllUpdates(mapNestedUpdateList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createSetAdd(String element){
    	List<String> elements = new ArrayList<String>();
    	elements.add(0, element);
    	return createSetAdd(elements);
    };
    
    public ApbUpdateOperation createSetAdd(List<String> elements){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
    	for (String element : elements){
    		upBuilder.addAdds(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createSetRemove(String element){
    	List<String> elements = new ArrayList<String>();
    	elements.add(element);
    	return createSetAdd(elements);
    };
    
    public ApbUpdateOperation createSetRemove(List<String> elements){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
    	for (String element : elements){
    		upBuilder.addRems(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };

    
 //A getter for each CRDT that can be stored within a map
    
    public ApbGetCounterResp mapGetCounter(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.COUNTER;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);
    	ApbGetCounterResp counter = null;
    	if (! (readResponse == null)){
    		counter = readResponse.getCounter();
    	}
        return counter;
    }
    
    public ApbGetIntegerResp mapGetInteger(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.INTEGER;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);
    	ApbGetIntegerResp integer = null;
    	if (! (readResponse == null)){
    		integer = readResponse.getInt();
    	}
        return integer;
    }
    
    public ApbGetSetResp mapGetSet(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.ORSET;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);        
        ApbGetSetResp set = null;
    	if (! (readResponse == null)){
    		set = readResponse.getSet();
    	}
        return set;
    }
    
    public ApbGetMVRegResp mapGetMVReg(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.MVREG;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);      
        ApbGetMVRegResp reg = null;
    	if (! (readResponse == null)){
    		reg = readResponse.getMvreg();
    	}
        return reg;
    }
    
    public ApbGetRegResp mapGetReg(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.LWWREG;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);  
        ApbGetRegResp reg = null;
    	if (! (readResponse == null)){
    		reg = readResponse.getReg();
    	}
        return reg;
    }
    
    public ApbGetMapResp mapGetMap(String name, String bucket, String key){
    	CRDT_type type = CRDT_type.AWMAP;
    	ApbReadObjectResp readResponse = getMapEntryHelper(name, bucket, key, type);
        ApbGetMapResp map = null;
    	if (! (readResponse == null)){
    		map = readResponse.getMap();
    	}
        return map;
    }
    
    public ApbReadObjectResp getMapEntryHelper(String name, String bucket, String key, CRDT_type type){
    	ApbGetMapResp map = readMap(name, bucket);
    	List<ApbMapEntry> entriesList = map.getEntriesList();
    	ApbReadObjectResp entry = null;
    	for (ApbMapEntry e : entriesList){
    		if (e.getKey().getKey().equals(ByteString.copyFromUtf8(key)) && e.getKey().getType().equals(type)){
    			entry = e.getValue();	
    		}
       	}
    	return entry;
    }
}