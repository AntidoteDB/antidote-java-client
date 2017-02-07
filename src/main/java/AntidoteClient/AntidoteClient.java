package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import java.util.*;

/**
 * The Class AntidoteClient.
 */
public class AntidoteClient {
    
    /**  Pool Manager. */
    private PoolManager poolManager;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
    }
    
    /**
     * Send message to database.
     *
     * @param requestMessage the update message
     * @return the response
     */
    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        return poolManager.sendMessage(requestMessage);
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
    protected void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type){
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
    protected ApbStaticReadObjectsResp readHelper(String name, String bucket, CRDT_type type){
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
		return new AntidoteMapUpdate(AntidoteType.CounterType, op);
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
		return new AntidoteMapUpdate(AntidoteType.IntegerType, op);
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
		return new AntidoteMapUpdate(AntidoteType.IntegerType, op);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param element the element that is added as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAddBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param elementList the elements that are added as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAddBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param element the element that is removed as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemoveBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param elementList the elements that are removed as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemoveBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param element the element that is added as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAddBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param elementList the elements that are added as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAddBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param element the element that is removed as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemoveBS(ByteString element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(element);
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param elementList the elements that are removed as ByteString
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemoveBS(List<ByteString> elementList){
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param element the element that is added as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set add.
	 *
	 * @param elementList the elements that are added as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetAdd(List<String> elementList){
		return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param element the element that is removed as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the OR-Set remove.
	 *
	 * @param elementList the elements that are removed as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createORSetRemove(List<String> elementList){
		return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);

	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param element the element that is added as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set add.
	 *
	 * @param elementList the elements that are added as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetAdd(List<String> elementList){
		return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param element the element that is removed as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(String element){
		List<ByteString> elementList = new ArrayList<>();
		elementList.add(ByteString.copyFromUtf8(element));
		return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the RW-Set remove.
	 *
	 * @param elementList the elements that are removed as String
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRWSetRemove(List<String> elementList){
		return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
	}
	
	/**
	 * Creates the set remove helper.
	 *
	 * @param elementList the element list
	 * @param type the type
	 * @param opNumber the operation number
	 * @return the antidote map update
	 */
	private AntidoteMapUpdate createSetRemoveHelper(List<ByteString> elementList, CRDT_type type, int opNumber){
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
		return createRegisterSetHelper(value, AntidoteType.LWWRegisterType);
	}
	
	/**
	 * Creates the MV-Register set.
	 *
	 * @param value the value to which the MV-Register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMVRegisterSet(ByteString value){
		return createRegisterSetHelper(value, AntidoteType.MVRegisterType);
	}
	
	/**
	 * Creates the register set.
	 *
	 * @param value the value to which the register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createRegisterSet(String value){
		return createRegisterSetHelper(ByteString.copyFromUtf8(value), AntidoteType.LWWRegisterType);
	}
	
	/**
	 * Creates the MV-Register set.
	 *
	 * @param value the value to which the MV-Register is set
	 * @return the antidote map update
	 */
	public AntidoteMapUpdate createMVRegisterSet(String value){
		return createRegisterSetHelper(ByteString.copyFromUtf8(value), AntidoteType.MVRegisterType);
	}
	
	/**
	 * Creates the register set helper.
	 *
	 * @param value the value
	 * @param type the type
	 * @return the antidote map update
	 */
	private AntidoteMapUpdate createRegisterSetHelper(ByteString value, CRDT_type type){
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
		return createMapUpdateHelper(key, updateList, AntidoteType.GMapType);
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
		return createMapUpdateHelper(key, updateList, AntidoteType.AWMapType);
	}
	
	/**
	 * Creates the map update helper.
	 *
	 * @param key the key
	 * @param updateList the update list
	 * @param mapType the map type
	 * @return the antidote map update
	 */
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
     * @param key the key
     * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
     * @return the antidote map update
     */
    public AntidoteMapUpdate createMapRemove(String key, CRDT_type type){
    	List<String> keyList = new ArrayList<>();
    	keyList.add(key);
    	return createMapRemove(keyList, type);
    }
	
    /**
     * Creates the map remove.
     *
     * @param keyList the key list
     * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
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
    	return new AntidoteMapUpdate(AntidoteType.AWMapType, op);
    }
    
    /**
     * String list to ByteString list.
     *
     * @param elementList the element list
     * @return the list
     */
    private List<ByteString> stringListToBSList(List<String> elementList){
    	List<ByteString> bsElementList = new ArrayList<>();
    	for (String element : elementList){
        	bsElementList.add(ByteString.copyFromUtf8(element));
        }
    	return bsElementList;
    }
}

