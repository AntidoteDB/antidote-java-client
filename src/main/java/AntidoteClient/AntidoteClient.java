/*
 * 
 */
package main.java.AntidoteClient;
import static java.lang.Math.toIntExact;
import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import java.util.*;
import java.io.*;
import java.net.Socket;

// TODO: Auto-generated Javadoc
/**
 * The Class AntidoteClient.
 */
public class AntidoteClient {
    
    /** The socket. */
    private Socket socket;
    
    /** The host. */
    private String host;
    
    /** The port. */
    private int port;

    /**
     * Instantiates a new antidote client.
     *
     * @param host the host
     * @param port the port
     */
    public AntidoteClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    /**
     * Send message.
     *
     * @param requestMessage the request message
     * @return the antidote message
     */
    public AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(requestMessage.getLength());
            dataOutputStream.writeByte(requestMessage.getCode());
            requestMessage.getMessage().writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            socket.close();

            return new AntidoteMessage(responseLength, responseCode, messageData);

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    /**
     * Update counter.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the inc
     */
    public void updateCounter(String name, String bucket, int inc) {

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

        int messageCode = 122; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, counterUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }

    /**
     * Read counter.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the apb get counter resp
     */
    public AntidoteCounter readCounter(String name, String bucket) {

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

        int messageCode = 123; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetCounterResp counter = readResponse.getObjects().getObjects(0).getCounter();
            AntidoteCounter antidoteCounter = new AntidoteCounter(name, bucket, counter.getValue(), this);
            return antidoteCounter;
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }
    }
    
    /**
     * Removes the set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     */
    public void removeSetElement(String name, String bucket, List<String> elements){
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

    /**
     * Adds the set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     */
    public void addSetElement(String name, String bucket, List<String> elements){
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
    
    /**
     * Update set helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param setUpdateInstruction the set update instruction
     */
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
        int messageCode = 122; // todo: change this to enum

        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, setUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read set.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the apb get set resp
     */
    public AntidoteSet readSet(String name, String bucket) {

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

        int messageCode = 123; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetSetResp set = readResponse.getObjects().getObjects(0).getSet();
            List<String> entriesList = new ArrayList<String>();
            for (ByteString e : set.getValueList()){
            	entriesList.add(e.toStringUtf8());
            }
            AntidoteSet antidoteSet = new AntidoteSet(name, bucket, entriesList, this);
            return antidoteSet;
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }     
    }
    
    /**
     * Update register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     */
    public void updateRegister(String name, String bucket, String value){

        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.LWWREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);

    }
    
    /**
     * Update MV register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     */
    public void updateMVRegister(String name, String bucket, String value){
    	
        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.MVREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);
    }
    
    /**
     * Update register helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param regObject the reg object
     */
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

        int messageCode = 122; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, regUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read register.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the apb get reg resp
     */
    public AntidoteRegister readRegister(String name, String bucket) {

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

        int messageCode = 123; // todo: change this to enum

        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetRegResp reg = readResponse.getObjects().getObjects(0).getReg();
            return new AntidoteRegister(name, bucket, reg.getValue().toStringUtf8(), this);
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }   
    }
    
    /**
     * Read MV register.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the apb get MV reg resp
     */
    public AntidoteMVRegister readMVRegister(String name, String bucket) {
    	
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
        
        int messageCode = 123; // todo: change this to enum       
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetMVRegResp reg = readResponse.getObjects().getObjects(0).getMvreg();         
            List<String> entriesList = new ArrayList<String>();
            for (ByteString e : reg.getValuesList()){
            	entriesList.add(e.toStringUtf8());
            }
            AntidoteMVRegister antidoteMVRegister = new AntidoteMVRegister(name, bucket, entriesList, this);
            return antidoteMVRegister;
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }   
    }
    
    /**
     * Increment integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the inc
     */
    public void incrementInteger(String name, String bucket, int inc) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(inc); // Set increment
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    /**
     * Sets the integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @param number the number
     */
    public void setInteger(String name, String bucket, int number) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(number); //Set the integer to this value
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    /**
     * Update integer helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param intUpdateInstruction the int update instruction
     */
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

        int messageCode = 122; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, intUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the apb get integer resp
     */
    public AntidoteInteger readInteger(String name, String bucket) {

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

        int messageCode = 123; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetIntegerResp number = readResponse.getObjects().getObjects(0).getInt();
            return new AntidoteInteger(name, bucket, toIntExact(number.getValue()), this);
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }   
    }
    
    /**
     * Update map.
     *
     * @param name the name
     * @param bucket the bucket
     * @param key the key
     * @param type the type
     * @param update the update
     */
    
	/**
	 * Update map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates
	 */
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

        int messageCode = 122; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, mapUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }        
    }
    
    /**
     * Removes the map element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param keys the keys
     */
    public void removeMapElement(String name, String bucket, List<ApbMapKey> keys) {

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

        int messageCode = 122; // todo: change this to enum
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, mapUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }  	       
    }
   	
	/**
	 * Read map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @return the apb get map resp
	 */
    
	public AntidoteMap readMap(String name, String bucket) {

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

        int messageCode = 123; // todo: change this to enum    
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(messageCode, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetMapResp map = readResponse.getObjects().getObjects(0).getMap();
            List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
            apbEntryList = map.getEntriesList();
            List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
    		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
            antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList);     
            return new AntidoteMap(name, bucket, antidoteEntryList, this);
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }      
    }
	
	public List<AntidoteMapEntry> readMapHelper(String name, String bucket, List<ApbMapKey> path, List<ApbMapEntry> apbEntryList){
		List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
		path.add( null);
		for (ApbMapEntry e: apbEntryList){
        	path.set(path.size()-1, e.getKey());
        	List<ApbMapKey> path2 = new ArrayList<ApbMapKey>();
        	switch(e.getKey().getType()){
        		case COUNTER :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapCounterEntry(e.getValue().getCounter().getValue(), this, name, bucket, path2));
             		break;
         		case ORSET :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
         			List<String> entryList = new ArrayList<String>();
         	        for (ByteString elt : e.getValue().getSet().getValueList()){
         	        	entryList.add(elt.toStringUtf8());
         	        }
             		antidoteEntryList.add(new AntidoteMapSetEntry(entryList, this, name, bucket, path2));
             		break;
         		case AWMAP :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapMapEntry(
             				readMapHelper(name, bucket, path, e.getValue().getMap().getEntriesList()), this, name, bucket, path2));
             		break;
         		case INTEGER:
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapIntegerEntry(toIntExact(e.getValue().getInt().getValue()), this, name, bucket, path2));
             		break;
         		case LWWREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapRegisterEntry(e.getValue().getReg().getValue().toStringUtf8(), this, name, bucket, path2));
             		break;
         		case MVREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
        			List<String> values = new ArrayList<String>();
        			for (ByteString elt : e.getValue().getMvreg().getValuesList()){
        				values.add(elt.toStringUtf8());
        			}
             		antidoteEntryList.add(new AntidoteMapMVRegisterEntry(values, this, name, bucket, path2));
             		break;
			default:
				break;
         	}
        }
		path.remove(0);
		return antidoteEntryList;
	}

    /**
     * Creates the counter increment operation.
     *
     * @param value the value
     * @return the apb update operation
     */
    public ApbUpdateOperation createCounterIncrementOperation(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbCounterUpdate.Builder upBuilder = ApbCounterUpdate.newBuilder();
    	upBuilder.setInc(value);
    	ApbCounterUpdate up = upBuilder.build();
    	opBuilder.setCounterop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    /**
     * Creates the integer increment operation.
     *
     * @param value the value
     * @return the apb update operation
     */
    public ApbUpdateOperation createIntegerIncrementOperation(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setInc(value);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    /**
     * Creates the integer set operation.
     *
     * @param value the value
     * @return the apb update operation
     */
    public ApbUpdateOperation createIntegerSetOperation(int value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setSet(value);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    /**
     * Creates the register set operation.
     *
     * @param value the value
     * @return the apb update operation
     */
    public ApbUpdateOperation createRegisterSetOperation(String value){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbRegUpdate.Builder upBuilder = ApbRegUpdate.newBuilder();
    	upBuilder.setValue(ByteString.copyFromUtf8(value));
    	ApbRegUpdate up = upBuilder.build();
    	opBuilder.setRegop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    /**
     * Creates the map update operation.
     *
     * @param key the key
     * @param updates the updates
     * @return the apb update operation
     */
    public ApbUpdateOperation createMapUpdateOperation(ApbMapKey key, List <ApbUpdateOperation> updates){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	
    	ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder();
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        for (ApbUpdateOperation update : updates){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(key);
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(mapNestedUpdate);
        }
    	upBuilder.addAllUpdates(mapNestedUpdateList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };
    
    public ApbUpdateOperation createMapRemoveOperation(List<ApbMapKey> keyList){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	upBuilder.addAllRemovedKeys(keyList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return op;
    };

    /**
     * Creates the set add element operation.
     *
     * @param elements the elements
     * @return the apb update operation
     */
    public ApbUpdateOperation createSetAddElementOperation(List<String> elements){
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
    
    /**
     * Creates the set remove element operation.
     *
     * @param elements the elements
     * @return the apb update operation
     */
    public ApbUpdateOperation createSetRemoveElementOperation(List<String> elements){
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
}