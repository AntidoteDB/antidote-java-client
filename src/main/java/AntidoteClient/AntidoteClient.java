package main.java.AntidoteClient;
import static java.lang.Math.toIntExact;
import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import java.util.*;
import java.io.*;
import java.net.Socket;

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
     * Send message to database.
     *
     * @param requestMessage the update message
     * @return the response
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

    //methods for updating and reading from the database
    
    /**
     * Update counter in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the increment, by which the counter shall be incremented
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, counterUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }

    /**
     * Read counter from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote counter
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
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
     * Removes the OR-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be removed
     */
    public void removeORSetElement(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllRems(elementsByteString);
        updateORSetHelper(name, bucket, setUpdateInstruction);
    }

    /**
     * Adds the OR-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be added
     */
    public void addORSetElement(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllAdds(elementsByteString);
        updateORSetHelper(name, bucket, setUpdateInstruction);
    }
    
    /**
     * Helper method for the common parts of the preceding methods.
     *
     * @param name the name
     * @param bucket the bucket
     * @param setUpdateInstruction the set update instruction
     */
    public void updateORSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction){
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, setUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read OR-Set from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote OR-Set
     */
    public AntidoteORSet readORSet(String name, String bucket) {

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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetSetResp set = readResponse.getObjects().getObjects(0).getSet();
            List<String> entriesList = new ArrayList<String>();
            for (ByteString e : set.getValueList()){
            	entriesList.add(e.toStringUtf8());
            }
            AntidoteORSet antidoteSet = new AntidoteORSet(name, bucket, entriesList, this);
            return antidoteSet;
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }     
    }
    
    /**
     * Removes the RW-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be removed
     */
    public void removeRWSetElement(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllRems(elementsByteString);
        updateRWSetHelper(name, bucket, setUpdateInstruction);
    }

    /**
     * Adds the RW-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be added
     */
    public void addRWSetElement(String name, String bucket, List<String> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllAdds(elementsByteString);
        updateRWSetHelper(name, bucket, setUpdateInstruction);
    }
    
    /**
     * Helper method for the common part of the two preceding methods.
     *
     * @param name the name
     * @param bucket the bucket
     * @param setUpdateInstruction the set update instruction
     */
    public void updateRWSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction){
        ApbStaticUpdateObjects.Builder setUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder setObject = ApbBoundObject.newBuilder(); // The object in the message
        setObject.setKey(ByteString.copyFromUtf8(name));
        setObject.setType(CRDT_type.RWSET);
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, setUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read RW-Set from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote RW-Set
     */
    public AntidoteRWSet readRWSet(String name, String bucket) {

        ApbBoundObject.Builder setObject = ApbBoundObject.newBuilder(); // The object in the message
        setObject.setKey(ByteString.copyFromUtf8(name));
        setObject.setType(CRDT_type.RWSET);
        setObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(setObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetSetResp set = readResponse.getObjects().getObjects(0).getSet();
            List<String> entriesList = new ArrayList<String>();
            for (ByteString e : set.getValueList()){
            	entriesList.add(e.toStringUtf8());
            }
            AntidoteRWSet antidoteSet = new AntidoteRWSet(name, bucket, entriesList, this);
            return antidoteSet;
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }     
    }
    
    /**
     * Update register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateRegister(String name, String bucket, String value){

        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.LWWREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);

    }
    
    /**
     * Update MV register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateMVRegister(String name, String bucket, String value){
    	
        ApbBoundObject.Builder regObject = ApbBoundObject.newBuilder(); // The object in the message
        regObject.setKey(ByteString.copyFromUtf8(name));
        regObject.setType(CRDT_type.MVREG);
        regObject.setBucket(ByteString.copyFromUtf8(bucket));
        updateRegisterHelper(name, bucket, value, regObject);
    }
    
    /**
     * Helper method for the common part of the two preceding methods
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, regUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read register from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote register
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
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
     * Read MV register from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote MV-Register
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
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
     * Increment integer in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the increment, by which the integer is incremented
     */
    public void incrementInteger(String name, String bucket, int inc) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(inc); // Set increment
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    /**
     * Sets the integer in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param number the number, to which the integer is set
     */
    public void setInteger(String name, String bucket, int number) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(number); //Set the integer to this value
        updateIntegerHelper(name, bucket, intUpdateInstruction);
    }
    
    /**
     * Helper method for the common part of the two preceding methods
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, intUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read integer from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote integer
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
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
	 * Update AW-Map in database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates executed in that AW-Map
	 */
	public void updateAWMap(String name, String bucket, ApbMapKey mapKey, List<ApbUpdateOperation> updates) {

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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, mapUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }        
    }
	
	/**
	 * Update G-Map in database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates executed in that G-Map
	 */
	public void updateGMap(String name, String bucket, ApbMapKey mapKey, List<ApbUpdateOperation> updates) {

        ApbStaticUpdateObjects.Builder mapUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder mapObject = ApbBoundObject.newBuilder(); // The object in the message
        mapObject.setKey(ByteString.copyFromUtf8(name));
        mapObject.setType(CRDT_type.GMAP);
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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, mapUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }        
    }
    
    /**
     * Removes the AW-Map entry in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param keys the keys of all elements in that Map which are removed
     */
    public void removeAWMapEntry(String name, String bucket, List<ApbMapKey> keys) {

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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, mapUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }  	       
    }
    
	/**
	 * Read AW map from database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @return the antidote AW-Map
	 */
	public AntidoteAWMap readAWMap(String name, String bucket) {

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
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
            ApbGetMapResp map = readResponse.getObjects().getObjects(0).getMap();
            List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
            apbEntryList = map.getEntriesList();
            List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
    		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
            antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.AWMAP);     
            return new AntidoteAWMap(name, bucket, antidoteEntryList, this);
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }      
    }
	
	/**
	 * Read G-Map from database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @return the antidote G-Map
	 */
	public AntidoteGMap readGMap(String name, String bucket) {

        ApbBoundObject.Builder intObject = ApbBoundObject.newBuilder(); // The object in the message
        intObject.setKey(ByteString.copyFromUtf8(name));
        intObject.setType(CRDT_type.GMAP);
        intObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(intObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();        
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        try {
            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage()); // TODO: This doesn't work
            ApbGetMapResp map = readResponse.getObjects().getObjects(0).getMap();
            List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
            apbEntryList = map.getEntriesList();
            List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
    		List<ApbMapKey> path = new ArrayList<ApbMapKey>();
            antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.GMAP);     
            return new AntidoteGMap(name, bucket, antidoteEntryList, this);
        } catch (Exception e ) {
            System.out.println(e);
            return null;
        }              
    }
	
	/**
	 * Helper method for the common part of reading both kinds of maps.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path storing the key and type of all inner maps leading to the entry. This is needed when storing Map entries in variables 
	 * that are a subclass of AntidoteMapEntry if we want to give them an update method.
	 * @param apbEntryList the ApbEntryList of the map, which is transformed into AntidoteMapEntries
	 * @param outerMapType the type of the outer Map (G-Map or AW-Map). This is given to the AntidoteMapEntries as the type of the outermost Map is 
	 * not stored in the path
	 * @return the list of AntidoteMapEntries
	 */
	public List<AntidoteMapEntry> readMapHelper(String name, String bucket, List<ApbMapKey> path, List<ApbMapEntry> apbEntryList, CRDT_type outerMapType){
		List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
		path.add(null);
		for (ApbMapEntry e: apbEntryList){
        	path.set(path.size()-1, e.getKey());
        	List<ApbMapKey> path2 = new ArrayList<ApbMapKey>();
        	switch(e.getKey().getType()){
        		case COUNTER :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapCounterEntry(e.getValue().getCounter().getValue(), this, name, bucket, path2, outerMapType));
             		break;
         		case ORSET :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
         			List<String> orSetEntryList = new ArrayList<String>();
         	        for (ByteString elt : e.getValue().getSet().getValueList()){
         	        	orSetEntryList.add(elt.toStringUtf8());
         	        }
             		antidoteEntryList.add(new AntidoteMapORSetEntry(orSetEntryList, this, name, bucket, path2, outerMapType));
             		break;
         		case RWSET : 
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
         			List<String> rwSetEntryList = new ArrayList<String>();
         	        for (ByteString elt : e.getValue().getSet().getValueList()){
         	        	rwSetEntryList.add(elt.toStringUtf8());
         	        }
             		antidoteEntryList.add(new AntidoteMapRWSetEntry(rwSetEntryList, this, name, bucket, path2, outerMapType));
             		break;
         		case AWMAP :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapAWMapEntry(
             				readMapHelper(name, bucket, path, e.getValue().getMap().getEntriesList(), outerMapType), this, name, bucket, path2, outerMapType));
             		break;
         		case INTEGER :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapIntegerEntry(toIntExact(e.getValue().getInt().getValue()), this, name, bucket, path2, outerMapType));
             		break;
         		case LWWREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapRegisterEntry(e.getValue().getReg().getValue().toStringUtf8(), this, name, bucket, path2, outerMapType));
             		break;
         		case MVREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
        			List<String> values = new ArrayList<String>();
        			for (ByteString elt : e.getValue().getMvreg().getValuesList()){
        				values.add(elt.toStringUtf8());
        			}
             		antidoteEntryList.add(new AntidoteMapMVRegisterEntry(values, this, name, bucket, path2, outerMapType));
             		break;
         		case GMAP : 
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapGMapEntry(
             				readMapHelper(name, bucket, path, e.getValue().getMap().getEntriesList(), outerMapType), this, name, bucket, path2, outerMapType));
             		break;
         	}
        }
		path.remove(0);
		return antidoteEntryList;
	}
	
	//Methods for creating AntidoteMapUpdates, which are given to AntidoteMaps and AntidoteMapMapEntries in order to execute updates
    
    /**
	 * Creates the counter increment.
	 *
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createCounterIncrement(){
		return createCounterIncrement(1);
	}
	
	/**
	 * Creates the counter increment.
	 *
	 * @param inc the value by which the counter is incremented
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createCounterIncrement(int inc){
	    ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
	    ApbCounterUpdate.Builder upBuilder = ApbCounterUpdate.newBuilder();
	    upBuilder.setInc(inc);
	    ApbCounterUpdate up = upBuilder.build();
	    opBuilder.setCounterop(up);
	    ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.COUNTER, op);
	}
	
	/**
	 * Creates the integer increment.
	 *
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createIntegerIncrement(){
		return createIntegerIncrement(1);
	}
	
	/**
	 * Creates the integer increment.
	 *
	 * @param inc the value by which the integer is incremented
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createIntegerIncrement(int inc){
		if (inc == 0){
			//needed for the Integer case in AntidoteMap's switch statement, where a set operation is assumed when the increment is 0
			throw new IllegalArgumentException("You can't increment by 0");
		}
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setInc(inc);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.INTEGER, op);
	}
	
	/**
	 * Creates the integer set operation.
	 *
	 * @param value the value to which the integer is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createIntegerSet(int value){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbIntegerUpdate.Builder upBuilder = ApbIntegerUpdate.newBuilder();
    	upBuilder.setSet(value);
    	ApbIntegerUpdate up = upBuilder.build();
    	opBuilder.setIntegerop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.INTEGER, op);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param element the element that is added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createORSetAdd(elementList);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(List<String> elementList){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
    	for (String element : elementList){
    		upBuilder.addAdds(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.ORSET, op);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createORSetRemove(elementList);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(List<String> elementList){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
    	for (String element : elementList){
    		upBuilder.addRems(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.ORSET, op);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param element the element that is added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createRWSetAdd(elementList);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(List<String> elementList){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
    	for (String element : elementList){
    		upBuilder.addAdds(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.RWSET, op);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		return createRWSetRemove(elementList);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(List<String> elementList){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
    	for (String element : elementList){
    		upBuilder.addRems(ByteString.copyFromUtf8(element));
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.RWSET, op);
	}
	
	/**
	 * Creates the register set.
	 *
	 * @param value the value to which the register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRegisterSet(String value){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbRegUpdate.Builder upBuilder = ApbRegUpdate.newBuilder();
    	upBuilder.setValue(ByteString.copyFromUtf8(value));
    	ApbRegUpdate up = upBuilder.build();
    	opBuilder.setRegop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.LWWREG, op);
	}
	
	/**
	 * Creates the MV-Register set.
	 *
	 * @param value the value to which the MV-Register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMVRegisterSet(String value){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbRegUpdate.Builder upBuilder = ApbRegUpdate.newBuilder();
    	upBuilder.setValue(ByteString.copyFromUtf8(value));
    	ApbRegUpdate up = upBuilder.build();
    	opBuilder.setRegop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.MVREG, op);
	}
	
	/**
	 * Creates the G-Map update.
	 *
	 * @param key the key of the entry to be updated
	 * @param update the update which is executed on a particular entry of the map
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createGMapUpdate(String key, AntidoteMapUpdate update) {
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		return createGMapUpdate(key, updateList);
	}
	
	/**
	 * Creates the G-Map update.
	 *
	 * @param key the key of the entry to be updated
	 * @param updateList the list of updates which are executed on a particular entry of the map
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createGMapUpdate(String key, List<AntidoteMapUpdate> updateList) {
		CRDT_type type = updateList.get(0).getType();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new IllegalArgumentException("Different types detected, only one type allowed");
			}
			apbUpdateList.add(u.getOperation());
		}
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		apbKeyBuilder.setType(type);
		ApbMapKey apbKey = apbKeyBuilder.build();
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	
    	ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder();
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        for (ApbUpdateOperation update : apbUpdateList){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(apbKey);
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(mapNestedUpdate);
        }
    	upBuilder.addAllUpdates(mapNestedUpdateList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.GMAP, op);
	}
	
	/**
	 * Creates the AW-Map update.
	 *
	 * @param key the key of the entry to be updated
	 * @param update the update which is executed on a particular entry of the map
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createAWMapUpdate(String key, AntidoteMapUpdate update) {
		List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
		updateList.add(update);
		return createAWMapUpdate(key, updateList);
	}
	
	/**
	 * Creates the AW-Map update.
	 *
	 * @param the key of the entry to be updated
	 * @param updateList the list of updates which are executed on a particular entry of the map
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createAWMapUpdate(String key, List<AntidoteMapUpdate> updateList) {
		CRDT_type type = updateList.get(0).getType();
		List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
		for (AntidoteMapUpdate u : updateList){
			if (!(type.equals(u.getType()))){
				throw new IllegalArgumentException("Different types detected, only one type allowed");
			}
			apbUpdateList.add(u.getOperation());
		}
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(ByteString.copyFromUtf8(key));
		apbKeyBuilder.setType(type);
		ApbMapKey apbKey = apbKeyBuilder.build();
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	
    	ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder();
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        for (ApbUpdateOperation update : apbUpdateList){
        	mapNestedUpdateBuilder.setUpdate(update);
        	mapNestedUpdateBuilder.setKey(apbKey);
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(mapNestedUpdate);
        }
    	upBuilder.addAllUpdates(mapNestedUpdateList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(CRDT_type.AWMAP, op);
	}
	
    /**
     * Creates the actual remove update. G-Maps are grow only, so this is only called for AW-Maps
     *
     * @param keyList the keys of all elements that are removed
     * @return the antidote map update
     */
    public AntidoteMapUpdate createMapRemove(List<ApbMapKey> keyList){
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	upBuilder.addAllRemovedKeys(keyList);
    	ApbMapUpdate up = upBuilder.build();
    	opBuilder.setMapop(up);
    	ApbUpdateOperation op = opBuilder.build();
    	return new AntidoteMapUpdate(CRDT_type.AWMAP, op);
    }
    
	/**
	 * Creates the counter remove.
	 *
	 * @param key the key of the counter to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapCounterRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapCounterRemove(keyList);
	}
	
	/**
	 * Creates the counter remove.
	 *
	 * @param keyList the keys of the counters to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapCounterRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.COUNTER);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the integer remove.
	 *
	 * @param key the key of the integer to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapIntegerRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapIntegerRemove(keyList);
	}
	
	/**
	 * Creates the integer remove.
	 *
	 * @param keyList the keys of the integers to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapIntegerRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.INTEGER);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param key the key of the OR-Set to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapORSetRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapORSetRemove(keyList);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param keyList the the keys of the OR-Sets to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapORSetRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.ORSET);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param key the key of the RW-Set to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRWSetRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapRWSetRemove(keyList);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param keyList the keys of the RW-Sets to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRWSetRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.RWSET);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the register remove.
	 *
	 * @param key the key of the register to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRegisterRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapRegisterRemove(keyList);
	}
	
	/**
	 * Creates the register remove.
	 *
	 * @param keyList the the keys of the registers to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRegisterRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.LWWREG);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the MV-Register remove.
	 *
	 * @param key the key of the MV-Register to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapMVRegisterRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapMVRegisterRemove(keyList);
	}
	
	/**
	 * Creates the MV-Register remove.
	 *
	 * @param keyList the keys of the MV-Registers to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapMVRegisterRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.MVREG);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the AW-Map remove.
	 *
	 * @param key the key of the AW-Map to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapAWMapRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapAWMapRemove(keyList);
	}
	
	/**
	 * Creates the AW-Map remove.
	 *
	 * @param keyList the keys of the AW-Maps to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapAWMapRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.AWMAP);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
	
	/**
	 * Creates the G-Map remove.
	 *
	 * @param key the key of the G-Map to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapGMapRemove(String key) {
		List<String> keyList = new ArrayList<String>();
		keyList.add(key);
		return createMapAWMapRemove(keyList);
	}
	
	/**
	 * Creates the G-Map remove.
	 *
	 * @param keyList the keys of the G-Maps to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapGMapRemove(List<String> keyList) {
		List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(CRDT_type.GMAP);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
		return createMapRemove(apbKeyList);		
	}
}