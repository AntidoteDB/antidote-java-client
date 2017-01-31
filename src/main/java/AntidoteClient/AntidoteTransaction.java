package main.java.AntidoteClient;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;

/**
 * Created by george on 1/21/17.
 */
public class AntidoteTransaction {

    /** The antidote client. */
    private AntidoteClient antidoteClient;

    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteTransaction(AntidoteClient antidoteClient){
      this.antidoteClient=antidoteClient;
    }

    /**
     * Start transaction.
     *
     * @return the byte string
     */
    public ByteString startTransaction(){
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readwriteTransaction = ApbStartTransaction.newBuilder();
        readwriteTransaction.setProperties(transactionProperties);

        ApbStartTransaction startTransactionMessage = readwriteTransaction.build();
        AntidoteMessage startMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStartTransaction, startTransactionMessage));

        try {
            ApbStartTransactionResp transactionResponse = ApbStartTransactionResp.parseFrom(startMessage.getMessage());
            System.out.println(transactionResponse);
            ByteString descriptor = transactionResponse.getTransactionDescriptor();
            return descriptor;
        }catch (Exception e){
            System.out.println(e);
            return null;
        }
    }

    /**
     * Commit transaction.
     *
     * @param descriptor the descriptor
     */
    public void commitTransaction(ByteString descriptor){
        ApbCommitTransaction.Builder commitTransaction = ApbCommitTransaction.newBuilder();
        commitTransaction.setTransactionDescriptor(descriptor);

        ApbCommitTransaction commitTransactionMessage = commitTransaction.build();
        AntidoteMessage message = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbCommitTransaction, commitTransactionMessage));

        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(message.getMessage());
            System.out.println(commitResponse);
        }catch (Exception e){
            System.out.println(e);
        }
    }
    
    /**
     * Update helper that has the generic part of the code.
     *
     * @param operation the operation
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @param descriptor the descriptor
     */
    private void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type, ByteString descriptor){
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
    	object.setKey(ByteString.copyFromUtf8(name));
    	object.setType(type);
    	object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);

        ApbUpdateObjects.Builder updateObject = ApbUpdateObjects.newBuilder();
        updateObject.addUpdates(updateInstruction);
        updateObject.setTransactionDescriptor(descriptor);

        ApbUpdateObjects updateObjectMessage = updateObject.build();
        antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbUpdateObjects, updateObjectMessage));
    }
    
    /**
     * Read helper that has the generic part of the code.
     *
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @param descriptor the descriptor
     * @return the apb read objects resp
     */
    private ApbReadObjectsResp readHelper(String name, String bucket, CRDT_type type, ByteString descriptor){
    	
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
    	object.setKey(ByteString.copyFromUtf8(name));
    	object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbReadObjects.Builder readObject = ApbReadObjects.newBuilder();
        readObject.addBoundobjects(object);
        readObject.setTransactionDescriptor(descriptor);

        ApbReadObjects readObjectsMessage = readObject.build();
        AntidoteMessage readMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbReadObjects, readObjectsMessage));
        
        ApbReadObjectsResp readResponse = null;
        try {
            readResponse = ApbReadObjectsResp.parseFrom(readMessage.getMessage());
        }catch (Exception e){
            System.out.println(e);
        }
        return readResponse;
    }

    /**
     * Update counter.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the inc
     * @param descriptor the descriptor
     */
    public void updateCounterTransaction(String name, String bucket, int inc, ByteString descriptor){
    	ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);
        
        updateHelper(updateOperation, name, bucket, CRDT_type.COUNTER, descriptor);    
    }
    
    /**
     * Remove the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeORSetElementBSTransaction(String name, String bucket, ByteString element, ByteString descriptor){
    	List<ByteString> elementList = new ArrayList<>();
        elementList.add(element);
        removeORSetElementBSTransaction(name, bucket, elementList, descriptor);
    }

    /**
     * Add the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addORSetElementBSTransaction(String name, String bucket, ByteString element, ByteString descriptor){
        List<ByteString> elementList = new ArrayList<>();
        elementList.add(element);
        addORSetElementBSTransaction(name, bucket, elementList, descriptor);
    }
    
    /**
     * Remove the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeORSetElementBSTransaction(String name, String bucket, List<ByteString> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllRems(elements);
        updateORSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }

    /**
     * Add the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addORSetElementBSTransaction(String name, String bucket, List<ByteString> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllAdds(elements);
        updateORSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }
    
    /**
     * Remove the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeORSetElementTransaction(String name, String bucket, String element, ByteString descriptor){
    	List<String> elementList = new ArrayList<>();
        elementList.add(element);
        removeORSetElementTransaction(name, bucket, elementList, descriptor);
    }

    /**
     * Add the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addORSetElementTransaction(String name, String bucket, String element, ByteString descriptor){
        List<String> elementList = new ArrayList<>();
        elementList.add(element);
        addORSetElementTransaction(name, bucket, elementList, descriptor);
    }
    
    /**
     * Remove the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeORSetElementTransaction(String name, String bucket, List<String> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllRems(elementsByteString);
        updateORSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }

    /**
     * Add the OR set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addORSetElementTransaction(String name, String bucket, List<String> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllAdds(elementsByteString);
        updateORSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }
    
    /**
     * Update OR set helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param setUpdateInstruction the set update instruction
     * @param descriptor the descriptor
     */
    private void updateORSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction, ByteString descriptor){
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.ORSET, descriptor);   
    }
    
    /**
     * Remove the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeRWSetElementBSTransaction(String name, String bucket, ByteString element, ByteString descriptor){
    	List<ByteString> elementList = new ArrayList<>();
        elementList.add(element);
        removeRWSetElementBSTransaction(name, bucket, elementList, descriptor);
    }

    /**
     * Add the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addRWSetElementBSTransaction(String name, String bucket, ByteString element, ByteString descriptor){
        List<ByteString> elementList = new ArrayList<>();
        elementList.add(element);
        addRWSetElementBSTransaction(name, bucket, elementList, descriptor);
    }
    
    /**
     * Remove the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeRWSetElementBSTransaction(String name, String bucket, List<ByteString> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllRems(elements);
        updateRWSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }

    /**
     * Add the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addRWSetElementBSTransaction(String name, String bucket, List<ByteString> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        setUpdateInstruction.addAllAdds(elements);
        updateRWSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }
    
    /**
     * Remove the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeRWSetElementTransaction(String name, String bucket, String element, ByteString descriptor){
    	List<String> elementList = new ArrayList<>();
        elementList.add(element);
        removeRWSetElementTransaction(name, bucket, elementList, descriptor);
    }

    /**
     * Add the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addRWSetElementTransaction(String name, String bucket, String element, ByteString descriptor){
        List<String> elementList = new ArrayList<>();
        elementList.add(element);
        addRWSetElementTransaction(name, bucket, elementList, descriptor);
    }
    
    /**
     * Remove the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void removeRWSetElementTransaction(String name, String bucket, List<String> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllRems(elementsByteString);
        updateRWSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }

    /**
     * Add the RW set element.
     *
     * @param name the name
     * @param bucket the bucket
     * @param elements the elements
     * @param descriptor the descriptor
     */
    public void addRWSetElementTransaction(String name, String bucket, List<String> elements, ByteString descriptor){
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        List<ByteString> elementsByteString = new ArrayList<ByteString>();
        for (String e : elements){
        	elementsByteString.add(ByteString.copyFromUtf8(e));
        }
        setUpdateInstruction.addAllAdds(elementsByteString);
        updateRWSetHelper(name, bucket, setUpdateInstruction, descriptor);
    }
    
    /**
     * Update RW set helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param setUpdateInstruction the set update instruction
     * @param descriptor the descriptor
     */
    private void updateRWSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction, ByteString descriptor){
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);

        updateHelper(updateOperation, name, bucket, CRDT_type.RWSET, descriptor);   
    }
    
    /**
     * Update register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param descriptor the descriptor
     */
    public void updateRegisterTransaction(String name, String bucket, String value, ByteString descriptor){
        updateRegisterHelper(name, bucket, value, CRDT_type.LWWREG, descriptor);
    }
    
    /**
     * Update MV register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param descriptor the descriptor
     */
    public void updateMVRegisterTransaction(String name, String bucket, String value, ByteString descriptor){
        updateRegisterHelper(name, bucket, value, CRDT_type.MVREG, descriptor);
    }
    
    /**
     * Update register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param descriptor the descriptor
     */
    public void updateRegisterTransaction(String name, String bucket, ByteString value, ByteString descriptor){
        updateRegisterHelper(name, bucket, value, CRDT_type.LWWREG, descriptor);
    }
    
    /**
     * Update MV register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param descriptor the descriptor
     */
    public void updateMVRegisterTransaction(String name, String bucket, ByteString value, ByteString descriptor){
        updateRegisterHelper(name, bucket, value, CRDT_type.MVREG, descriptor);
    }
    
    /**
     * Update register helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param type the type
     * @param descriptor the descriptor
     */
    private void updateRegisterHelper(String name, String bucket, ByteString value, CRDT_type type, ByteString descriptor){
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(value);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        updateHelper(updateOperation, name, bucket, type, descriptor);
    }
    
    /**
     * Update register helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param value the value
     * @param type the type
     * @param descriptor the descriptor
     */
    private void updateRegisterHelper(String name, String bucket, String value, CRDT_type type, ByteString descriptor){
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(ByteString.copyFromUtf8(value));
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        updateHelper(updateOperation, name, bucket, type, descriptor);
    }
    
    /**
     * Increment integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @param inc the inc
     * @param descriptor the descriptor
     */
    public void incrementIntegerTransaction(String name, String bucket, int inc, ByteString descriptor) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(inc); // Set increment
        updateIntegerHelper(name, bucket, intUpdateInstruction, descriptor);
    }
    
    /**
     * Sets the integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @param number the number
     * @param descriptor the descriptor
     */
    public void setIntegerTransaction(String name, String bucket, int number, ByteString descriptor) {
        ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(number); //Set the integer to this value
        updateIntegerHelper(name, bucket, intUpdateInstruction, descriptor);
    }
    
    /**
     * Update integer helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param intUpdateInstruction the int update instruction
     * @param descriptor the descriptor
     */
    private void updateIntegerHelper(String name, String bucket, ApbIntegerUpdate.Builder intUpdateInstruction, ByteString descriptor) {
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.INTEGER, descriptor);
    }
    
    /**
     * Update AW map.
     *
     * @param name the name
     * @param bucket the bucket
     * @param mapKey the map key
     * @param update the update
     * @param descriptor the descriptor
     */
    public void updateAWMapTransaction(String name, String bucket, AntidoteMapKey mapKey, AntidoteMapUpdate update, ByteString descriptor) {
    	List<AntidoteMapUpdate> updates = new ArrayList<>();
    	updates.add(update);
    	updateMapTransactionHelper(name, bucket, mapKey, updates, CRDT_type.AWMAP, descriptor);
    }

	/**
	 * Update G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param update the update
	 * @param descriptor the descriptor
	 */
	public void updateGMapTransaction(String name, String bucket, AntidoteMapKey mapKey, AntidoteMapUpdate update, ByteString descriptor) { 
		List<AntidoteMapUpdate> updates = new ArrayList<>();
    	updates.add(update);
        updateMapTransactionHelper(name, bucket, mapKey, updates, CRDT_type.GMAP, descriptor);
    }
    
    /**
     * Update AW map.
     *
     * @param name the name
     * @param bucket the bucket
     * @param mapKey the map key
     * @param updates the updates
     * @param descriptor the descriptor
     */
    public void updateAWMapTransaction(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, ByteString descriptor) {
    	updateMapTransactionHelper(name, bucket, mapKey, updates, CRDT_type.AWMAP, descriptor);
    }

	/**
	 * Update G map.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param mapKey the map key
	 * @param updates the updates
	 * @param descriptor the descriptor
	 */
	public void updateGMapTransaction(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, ByteString descriptor) { 
        updateMapTransactionHelper(name, bucket, mapKey, updates, CRDT_type.GMAP, descriptor);
    }
	
	public void updateMapTransactionHelper(String name, String bucket, AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, CRDT_type mapType, ByteString descriptor) { 
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
        
        updateHelper(updateOperation, name, bucket, mapType, descriptor);
    }
    
    /**
     * Removes the AW map entry.
     *
     * @param name the name
     * @param bucket the bucket
     * @param keys the keys
     * @param descriptor the descriptor
     */
    public void removeAWMapEntryTransaction(String name, String bucket, List<AntidoteMapKey> keys, ByteString descriptor) {
        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        for (AntidoteMapKey key : keys){
        	mapUpdateInstruction.addRemovedKeys(key.getApbKey());
        }    
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        updateHelper(updateOperation, name, bucket, CRDT_type.AWMAP, descriptor);
    }

    /**
     * Read counter.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote counter
     */
    public AntidoteCounter readCounterTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetCounterResp counter = readHelper(name, bucket, CRDT_type.COUNTER, descriptor).getObjects(0).getCounter();
        AntidoteCounter antidoteCounter = new AntidoteCounter(name, bucket, counter.getValue(), antidoteClient);
        return antidoteCounter;
    }
    
    /**
     * Read OR set.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote OR set
     */
    public AntidoteORSet readORSetTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetSetResp set = readHelper(name, bucket, CRDT_type.ORSET, descriptor).getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteORSet antidoteSet = new AntidoteORSet(name, bucket, entriesList, antidoteClient);
        return antidoteSet;
    }
    
    /**
     * Read RW set.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote RW set
     */
    public AntidoteRWSet readRWSetTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetSetResp set = readHelper(name, bucket, CRDT_type.RWSET, descriptor).getObjects(0).getSet();
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : set.getValueList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteRWSet antidoteSet = new AntidoteRWSet(name, bucket, entriesList, antidoteClient);
        return antidoteSet;
    }
    
    /**
     * Read register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote register
     */
    public AntidoteRegister readRegisterTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetRegResp reg = readHelper(name, bucket, CRDT_type.LWWREG, descriptor).getObjects(0).getReg();
        return new AntidoteRegister(name, bucket, reg.getValue().toStringUtf8(), antidoteClient); 
    }
    
    /**
     * Read MV register.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote MV register
     */
    public AntidoteMVRegister readMVRegisterTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetMVRegResp reg = readHelper(name, bucket, CRDT_type.MVREG, descriptor).getObjects(0).getMvreg();         
        List<String> entriesList = new ArrayList<String>();
        for (ByteString e : reg.getValuesList()){
          	entriesList.add(e.toStringUtf8());
        }
        AntidoteMVRegister antidoteMVRegister = new AntidoteMVRegister(name, bucket, entriesList, antidoteClient);
        return antidoteMVRegister;  
    }
    
    /**
     * Read integer.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote integer
     */
    public AntidoteInteger readIntegerTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetIntegerResp number = readHelper(name, bucket, CRDT_type.INTEGER, descriptor).getObjects(0).getInt();
        return new AntidoteInteger(name, bucket, toIntExact(number.getValue()), antidoteClient);  
    }
    
    /**
     * Read AW map.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote AW map
     */
    public AntidoteAWMap readAWMapTransaction(String name, String bucket, ByteString descriptor){
    	ApbGetMapResp map = readHelper(name, bucket, CRDT_type.AWMAP, descriptor).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
    	List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.AWMAP);     
        return new AntidoteAWMap(name, bucket, antidoteEntryList, antidoteClient);   
    }
    
    /**
     * Read G map.
     *
     * @param name the name
     * @param bucket the bucket
     * @param descriptor the descriptor
     * @return the antidote G map
     */
    public AntidoteGMap readGMapTransaction(String name, String bucket, ByteString descriptor) {
        ApbGetMapResp map = readHelper(name, bucket, CRDT_type.GMAP, descriptor).getObjects(0).getMap();
        List<ApbMapEntry> apbEntryList = new ArrayList<ApbMapEntry>();
        apbEntryList = map.getEntriesList();
        List<AntidoteMapEntry> antidoteEntryList = new ArrayList<AntidoteMapEntry>();
        List<ApbMapKey> path = new ArrayList<ApbMapKey>();
        antidoteEntryList = readMapHelper(name, bucket, path, apbEntryList, CRDT_type.GMAP);     
        return new AntidoteGMap(name, bucket, antidoteEntryList, antidoteClient);             
    }
    
    /**
     * Read map helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param path the path
     * @param apbEntryList the apb entry list
     * @param outerMapType the outer map type
     * @return the list
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
             		antidoteEntryList.add(new AntidoteMapCounterEntry(e.getValue().getCounter().getValue(), antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case ORSET :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
         			List<String> orSetEntryList = new ArrayList<String>();
         	        for (ByteString elt : e.getValue().getSet().getValueList()){
         	        	orSetEntryList.add(elt.toStringUtf8());
         	        }
             		antidoteEntryList.add(new AntidoteMapORSetEntry(orSetEntryList, antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case RWSET : 
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
         			List<String> rwSetEntryList = new ArrayList<String>();
         	        for (ByteString elt : e.getValue().getSet().getValueList()){
         	        	rwSetEntryList.add(elt.toStringUtf8());
         	        }
             		antidoteEntryList.add(new AntidoteMapRWSetEntry(rwSetEntryList, antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case AWMAP :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapAWMapEntry(
             				readMapHelper(name, bucket, path, e.getValue().getMap().getEntriesList(), outerMapType), antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case INTEGER :
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapIntegerEntry(toIntExact(e.getValue().getInt().getValue()), antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case LWWREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapRegisterEntry(e.getValue().getReg().getValue().toStringUtf8(), antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case MVREG :
        			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
        			List<String> values = new ArrayList<String>();
        			for (ByteString elt : e.getValue().getMvreg().getValuesList()){
        				values.add(elt.toStringUtf8());
        			}
             		antidoteEntryList.add(new AntidoteMapMVRegisterEntry(values, antidoteClient, name, bucket, path2, outerMapType));
             		break;
         		case GMAP : 
         			path2 = new ArrayList<ApbMapKey>();
        			path2.addAll(path);
             		antidoteEntryList.add(new AntidoteMapGMapEntry(
             				readMapHelper(name, bucket, path, e.getValue().getMap().getEntriesList(), outerMapType), antidoteClient, name, bucket, path2, outerMapType));
             		break;
         	}
        }
		path.remove(0);
		return antidoteEntryList;
	}

}
