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
    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
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
     * Update helper.
     *
     * @param operation the operation
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     */
    private void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type){
    	ApbStaticUpdateObjects.Builder updateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
    	
        ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));
    	
    	ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        updateMessage.setTransaction(writeTransaction);
        updateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects counterUpdateMessageObject = updateMessage.build();
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, counterUpdateMessageObject));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            System.out.println(commitResponse);
        } catch (Exception e ) {
            System.out.println(e);
        }
    }
    
    /**
     * Read helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @return the apb static read objects resp
     */
    private ApbStaticReadObjectsResp readHelper(String name, String bucket, CRDT_type type){
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(object);

        ApbStaticReadObjects readMessageObject = readMessage.build();
        AntidoteMessage responseMessage = sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        
        ApbStaticReadObjectsResp readResponse = null;
		try {
			readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return readResponse;
    }
    
    /**
     * Update counter in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the increment, by which the counter shall be incremented
     */
    public void updateCounter(String name, String bucket, int inc) {
        ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);     
        updateHelper(updateOperation, name, bucket, CRDT_type.COUNTER);
    }
    
    /**
     * Read counter from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote counter
     */
    public AntidoteCounter readCounter(String name, String bucket) {
        ApbGetCounterResp counter = readHelper(name, bucket, CRDT_type.COUNTER).getObjects().getObjects(0).getCounter();
        AntidoteCounter antidoteCounter = new AntidoteCounter(name, bucket, counter.getValue(), this);
        return antidoteCounter;
    }
    
    /**
     * Removes the OR-Set element, given as ByteString, from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be removed
     */
    public void removeORSetElementBS(String name, String bucket, ByteString element){
    	List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        removeORSetElementBS(name, bucket, elements);
    }

    /**
     * Adds the OR-Set element, given as ByteString, to database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be added
     */
    public void addORSetElementBS(String name, String bucket, ByteString element){
        List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        addORSetElementBS(name, bucket, elements);
    }
    
    /**
     * Removes the OR-Set elements from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be removed
     */
    public void removeORSetElement(String name, String bucket, String element){
    	List<String> elements = new ArrayList<>();
        elements.add(element);
        removeORSetElement(name, bucket, elements);
    }
    
    /**
     * Adds the OR-Set element to database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be added
     */
    public void addORSetElement(String name, String bucket, String element){
        List<String> elements = new ArrayList<>();
        elements.add(element);
        addORSetElement(name, bucket, elements);
    }
    
    /**
     * Removes the OR-Set elements, given as ByteStrings, in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be removed
     */
    public void removeORSetElementBS(String name, String bucket, List<ByteString> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllRems(elements);
        updateORSetHelper(name, bucket, setUpdateInstruction);
    }

    /**
     * Adds the OR-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be added
     */
    public void addORSetElementBS(String name, String bucket, List<ByteString> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllAdds(elements);
        updateORSetHelper(name, bucket, setUpdateInstruction);
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
    private void updateORSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction){
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);

        updateHelper(updateOperation, name, bucket, CRDT_type.ORSET);
    }
    
    /**
     * Read RW-Set from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote RW-Set
     */
    public AntidoteORSet readORSet(String name, String bucket) {
        ApbGetSetResp set = readHelper(name, bucket, CRDT_type.ORSET).getObjects().getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteORSet antidoteSet = new AntidoteORSet(name, bucket, entriesList, this);
        return antidoteSet;
    }
    
    /**
     * Removes the RW-Set element, given as ByteString, from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be removed
     */
    public void removeRWSetElementBS(String name, String bucket, ByteString element){
    	List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        removeRWSetElementBS(name, bucket, elements);
    }

    /**
     * Adds the RW-Set element, given as ByteString, to database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be added
     */
    public void addRWSetElementBS(String name, String bucket, ByteString element){
        List<ByteString> elements = new ArrayList<>();
        elements.add(element);
        addRWSetElementBS(name, bucket, elements);
    }
    
    /**
     * Removes the RW-Set elements from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be removed
     */
    public void removeRWSetElement(String name, String bucket, String element){
    	List<String> elements = new ArrayList<>();
        elements.add(element);
        removeRWSetElement(name, bucket, elements);
    }
    
    /**
     * Adds the RW-Set element to database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param element the element to be added
     */
    public void addRWSetElement(String name, String bucket, String element){
        List<String> elements = new ArrayList<>();
        elements.add(element);
        addRWSetElement(name, bucket, elements);
    }
    
    /**
     * Removes the RW-Set elements, given as ByteStrings, in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be removed
     */
    public void removeRWSetElementBS(String name, String bucket, List<ByteString> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllRems(elements);
        updateRWSetHelper(name, bucket, setUpdateInstruction);
    }

    /**
     * Adds the RW-Set elements in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements to be added
     */
    public void addRWSetElementBS(String name, String bucket, List<ByteString> elements){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllAdds(elements);
        updateRWSetHelper(name, bucket, setUpdateInstruction);
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
    private void updateRWSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction){
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.RWSET);
    }
    
    /**
     * Read RW-Set from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote RW-Set
     */
    public AntidoteRWSet readRWSet(String name, String bucket) {
        ApbGetSetResp set = readHelper(name, bucket, CRDT_type.RWSET).getObjects().getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
           	entriesList.add(e.toStringUtf8());
        }
        AntidoteRWSet antidoteSet = new AntidoteRWSet(name, bucket, entriesList, this);
        return antidoteSet;  
    }
    
    /**
     * Update register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateRegister(String name, String bucket, String value){
        updateRegisterHelper(name, bucket, value, CRDT_type.LWWREG);
    }
    
    /**
     * Update MV register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateMVRegister(String name, String bucket, String value){
        updateRegisterHelper(name, bucket, value, CRDT_type.MVREG);
    }
    
    /**
     * Update register helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param type the type
     */
    private void updateRegisterHelper(String name, String bucket, String value, CRDT_type type){
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(ByteString.copyFromUtf8(value));

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        updateHelper(updateOperation, name, bucket, type);
    }
    
    /**
     * Update register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateRegisterBS(String name, String bucket, ByteString value){
        updateRegisterHelperBS(name, bucket, value, CRDT_type.LWWREG);
    }
    
    /**
     * Update MV register in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the new register value
     */
    public void updateMVRegisterBS(String name, String bucket, ByteString value){
        updateRegisterHelperBS(name, bucket, value, CRDT_type.MVREG);
    }
    
    /**
     * Update register helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param type the type
     */
    private void updateRegisterHelperBS(String name, String bucket, ByteString value, CRDT_type type){
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(value);

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        updateHelper(updateOperation, name, bucket, type);
    }
    
    /**
     * Read register from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote register
     */
    public AntidoteRegister readRegister(String name, String bucket) {
        ApbGetRegResp reg = readHelper(name, bucket, CRDT_type.LWWREG).getObjects().getObjects(0).getReg();
        return new AntidoteRegister(name, bucket, reg.getValue().toStringUtf8(), this); 
    }
    
    /**
     * Read MV register from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote MV-Register
     */
    public AntidoteMVRegister readMVRegister(String name, String bucket) {
        ApbGetMVRegResp reg = readHelper(name, bucket, CRDT_type.MVREG).getObjects().getObjects(0).getMvreg();         
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteMVRegister antidoteMVRegister = new AntidoteMVRegister(name, bucket, entriesList, this);
        return antidoteMVRegister;  
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
     * Helper method for the common part of the two preceding methods.
     *
     * @param name the name
     * @param bucket the bucket
     * @param intUpdateInstruction the int update instruction
     */
    private void updateIntegerHelper(String name, String bucket, ApbIntegerUpdate.Builder intUpdateInstruction) {
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.INTEGER);
    }
    
    /**
     * Read integer from database.
     *
     * @param name the name
     * @param bucket the bucket
     * @return the antidote integer
     */
    public AntidoteInteger readInteger(String name, String bucket) {
        ApbGetIntegerResp number = readHelper(name, bucket, CRDT_type.INTEGER).getObjects().getObjects(0).getInt();
        return new AntidoteInteger(name, bucket, toIntExact(number.getValue()), this);  
    }
   
    /**
	 * Update AW-Map in database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates executed in that AW-Map
	 */
	public void updateAWMap(String name, String bucket, AntidoteMapKey mapKey, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updates = new ArrayList<>();
        updates.add(update);
        updateAWMap(name, bucket, mapKey, updates);
    }
    
	/**
	 * Update AW-Map in database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates executed in that AW-Map
	 */
	public void updateAWMap(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates) {
        updateMapHelper(name, bucket, mapKey, updates, CRDT_type.AWMAP);
    }
	
	public void updateGMap(String name, String bucket, AntidoteMapKey mapKey, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updates = new ArrayList<>();
        updates.add(update);
        updateGMap(name, bucket, mapKey, updates);
    }
	
	/**
	 * Update G-Map in database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates executed in that G-Map
	 */
	public void updateGMap(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates) { 
        updateMapHelper(name, bucket, mapKey, updates, CRDT_type.GMAP);
    }
	
	public void updateMapHelper(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, CRDT_type mapType) { 
        ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate; 
        for (AntidoteMapUpdate update : updates){
        	mapNestedUpdateBuilder.setUpdate(update.getOperation());
        	mapNestedUpdateBuilder.setKey(mapKey.getApbKey());
        	mapNestedUpdate = mapNestedUpdateBuilder.build();
        	mapNestedUpdateList.add(mapNestedUpdate);
        }
        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        mapUpdateInstruction.addAllUpdates(mapNestedUpdateList);
            
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        
        updateHelper(updateOperation, name, bucket, mapType);
    }
	
	/**
     * Removes the AW-Map entry in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param key the key of the element in that Map which is removed
     */
    public void removeAWMapEntry(String name, String bucket, AntidoteMapKey key) {
        List<AntidoteMapKey> keys = new ArrayList<>();
        keys.add(key);
        removeAWMapEntry(name, bucket, keys);
    }
    
    /**
     * Removes the AW-Map entry in database.
     *
     * @param name the name
     * @param bucket the bucket
     * @param keys the keys of all elements in that Map which are removed
     */
    public void removeAWMapEntry(String name, String bucket, List<AntidoteMapKey> keys) {
        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapKey> apbKeys = new ArrayList<>();
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setType(keys.get(0).getType());
        for (AntidoteMapKey key : keys){
        	apbKeyBuilder.setKey(key.getKeyBS());
        	apbKeys.add(apbKeyBuilder.build());
        }
        mapUpdateInstruction.addAllRemovedKeys(apbKeys);      
        
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.AWMAP);
    }
    
	/**
	 * Read AW map from database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @return the antidote AW-Map
	 */
	public AntidoteAWMap readAWMap(String name, String bucket) {
        ApbGetMapResp map = readHelper(name, bucket, CRDT_type.AWMAP).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.AWMAP);     
        return new AntidoteAWMap(name, bucket, antidoteEntryList, this);   
    }
	
	/**
	 * Read G-Map from database.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @return the antidote G-Map
	 */
	public AntidoteGMap readGMap(String name, String bucket) {
        ApbGetMapResp map = readHelper(name, bucket, CRDT_type.GMAP).getObjects().getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
        List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.GMAP);     
        return new AntidoteGMap(name, bucket, antidoteEntryList, this);             
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
	private List<AntidoteMapEntry> readMapHelper(String name, String bucket, List<ApbMapKey> path, List<ApbMapEntry> apbEntryList, CRDT_type outerMapType){
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
	public AntidoteMapUpdate createORSetAddBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAddBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemoveBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemoveBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param element the element that is added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAddBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAddBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemoveBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemoveBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param element the element that is added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(List<String> elementList){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String element : elementList){
	    	bsElementList.add(ByteString.copyFromUtf8(element));
	    }
		return createSetRemoveHelper(bsElementList, CRDT_type.ORSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, CRDT_type.ORSET, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(List<String> elementList){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String element : elementList){
	    	bsElementList.add(ByteString.copyFromUtf8(element));
	    }
		return createSetRemoveHelper(bsElementList, CRDT_type.ORSET, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param element the element that is added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param elementList the elements that are added
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(List<String> elementList){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String element : elementList){
	    	bsElementList.add(ByteString.copyFromUtf8(element));
	    }
		return createSetRemoveHelper(bsElementList, CRDT_type.RWSET, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param element the element that is removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, CRDT_type.RWSET, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param elementList the elements that are removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(List<String> elementList){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String element : elementList){
	    	bsElementList.add(ByteString.copyFromUtf8(element));
	    }
		return createSetRemoveHelper(bsElementList, CRDT_type.RWSET, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the set remove helper.
	 *
	 * @param elementList the element list
	 * @param type the type
	 * @param opNumber the op number
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createSetRemoveHelper(List<ByteString> elementList, CRDT_type type, int opNumber){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
    	ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(opNumber);
    	if (opNumber == AntidoteSetOpType.SetRemove){
    	    for (ByteString element : elementList){
    	    	upBuilder.addRems(element);
    	    }
    	}
    	else if (opNumber == AntidoteSetOpType.SetAdd){
    		for (ByteString element : elementList){
    	    	upBuilder.addAdds(element);
    	    }
    	}
    	upBuilder.setOptype(opType);
    	ApbSetUpdate up = upBuilder.build();
    	opBuilder.setSetop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(type, op);
	}
	
	/**
	 * Creates the register set.
	 *
	 * @param value the value to which the register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRegisterSet(ByteString value){
		return createRegisterSetHelper(value, CRDT_type.LWWREG);
	}
	
	/**
	 * Creates the MV-Register set.
	 *
	 * @param value the value to which the MV-Register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMVRegisterSet(ByteString value){
		return createRegisterSetHelper(value, CRDT_type.MVREG);
	}
	
	/**
	 * Creates the register set.
	 *
	 * @param value the value to which the register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRegisterSet(String value){
		return createRegisterSetHelper(ByteString.copyFromUtf8(value), CRDT_type.LWWREG);
	}
	
	/**
	 * Creates the MV-Register set.
	 *
	 * @param value the value to which the MV-Register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMVRegisterSet(String value){
		return createRegisterSetHelper(ByteString.copyFromUtf8(value), CRDT_type.MVREG);
	}
	
	/**
	 * Creates the register set helper.
	 *
	 * @param value the value
	 * @param type the type
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRegisterSetHelper(ByteString value, CRDT_type type){
		ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbRegUpdate.Builder upBuilder = ApbRegUpdate.newBuilder();
    	upBuilder.setValue(value);
    	ApbRegUpdate up = upBuilder.build();
    	opBuilder.setRegop(up);
    	ApbUpdateOperation op = opBuilder.build();
		return new AntidoteMapUpdate(type, op);
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
		return createMapUpdateHelper(key, updateList, CRDT_type.GMAP);
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
	 * @param key the key
	 * @param updateList the list of updates which are executed on a particular entry of the map
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createAWMapUpdate(String key, List<AntidoteMapUpdate> updateList) {
		return createMapUpdateHelper(key, updateList, CRDT_type.AWMAP);
	}
	
	private AntidoteMapUpdate createMapUpdateHelper(String key, List<AntidoteMapUpdate> updateList, CRDT_type mapType) {
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
		return new AntidoteMapUpdate(mapType, op);
	}
	
    /**
     * Creates the map remove.
     *
     * @param keyList the key list
     * @param type the type
     * @return the antidote map update
     */
    public AntidoteMapUpdate createMapRemove(List<String> keyList, CRDT_type type){
    	List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
		ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
		keyBuilder.setType(type);
		for (String key : keyList){
			keyBuilder.setKey(ByteString.copyFromUtf8(key));
			apbKeyList.add(keyBuilder.build());
		}
    	ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
    	ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();
    	upBuilder.addAllRemovedKeys(apbKeyList);
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
		return createMapRemove(keyList, CRDT_type.COUNTER);		
	}
	
	/**
	 * Creates the map counter remove.
	 *
	 * @param keyList the key list
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapCounterRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.COUNTER);		
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
		return createMapRemove(keyList, CRDT_type.INTEGER);		
	}
	
	/**
	 * Creates the map integer remove.
	 *
	 * @param keyList the key list
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapIntegerRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.INTEGER);			
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
		return createMapRemove(keyList, CRDT_type.ORSET);		
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param keyList the the keys of the OR-Sets to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapORSetRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.ORSET);		
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
		return createMapRemove(keyList, CRDT_type.RWSET);		
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param keyList the keys of the RW-Sets to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRWSetRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.RWSET);			
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
		return createMapRemove(keyList, CRDT_type.LWWREG);		
	}
	
	/**
	 * Creates the register remove.
	 *
	 * @param keyList the the keys of the registers to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapRegisterRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.LWWREG);		
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
		return createMapRemove(keyList, CRDT_type.MVREG);		
	}
	
	/**
	 * Creates the MV-Register remove.
	 *
	 * @param keyList the keys of the MV-Registers to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapMVRegisterRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.MVREG);		
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
		return createMapRemove(keyList, CRDT_type.AWMAP);		
	}
	
	/**
	 * Creates the AW-Map remove.
	 *
	 * @param keyList the keys of the AW-Maps to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapAWMapRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.AWMAP);		
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
		return createMapRemove(keyList, CRDT_type.GMAP);		
	}
	
	/**
	 * Creates the G-Map remove.
	 *
	 * @param keyList the keys of the G-Maps to be removed
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMapGMapRemove(List<String> keyList) {
		return createMapRemove(keyList, CRDT_type.GMAP);		
	}
}