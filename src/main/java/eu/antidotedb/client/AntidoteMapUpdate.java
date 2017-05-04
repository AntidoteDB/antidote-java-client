package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteMapUpdate.
 */
public final class AntidoteMapUpdate {

    /**
     * The type.
     */
    private final CRDT_type type;

    /**
     * The operation.
     */
    private final ApbUpdateOperation operation;

    /**
     * Instantiates a new antidote map update.
     *
     * @param type      the type
     * @param operation the operation
     */
    public AntidoteMapUpdate(CRDT_type type, ApbUpdateOperation operation) {
        this.type = type;
        this.operation = operation;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public CRDT_type getType() {
        return type;
    }

    /**
     * Gets the operation.
     *
     * @return the operation
     */
    public ApbUpdateOperation getOperation() {
        return operation;
    }


    /**
     * Creates the counter increment.
     *
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createCounterIncrement() {
        return createCounterIncrement(1);
    }

    /**
     * Creates the counter increment.
     *
     * @param inc the value by which the counter is incremented
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createCounterIncrement(int inc) {
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
    public static AntidoteMapUpdate createIntegerIncrement() {
        return createIntegerIncrement(1);
    }

    /**
     * Creates the integer increment.
     *
     * @param inc the value by which the integer is incremented
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createIntegerIncrement(int inc) {
        if (inc == 0) {
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
    public static AntidoteMapUpdate createIntegerSet(int value) {
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
    public static AntidoteMapUpdate createORSetAddBS(ByteString element) {
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
    public static AntidoteMapUpdate createORSetAddBS(List<ByteString> elementList) {
        return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
    }

    /**
     * Creates the OR-Set remove.
     *
     * @param element the element that is removed as ByteString
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createORSetRemoveBS(ByteString element) {
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
    public static AntidoteMapUpdate createORSetRemoveBS(List<ByteString> elementList) {
        return createSetRemoveHelper(elementList, AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);
    }

    /**
     * Creates the RW-Set add.
     *
     * @param element the element that is added as ByteString
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createRWSetAddBS(ByteString element) {
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
    public static AntidoteMapUpdate createRWSetAddBS(List<ByteString> elementList) {
        return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
    }

    /**
     * Creates the RW-Set remove.
     *
     * @param element the element that is removed as ByteString
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createRWSetRemoveBS(ByteString element) {
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
    public static AntidoteMapUpdate createRWSetRemoveBS(List<ByteString> elementList) {
        return createSetRemoveHelper(elementList, AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
    }

    /**
     * Creates the OR-Set add.
     *
     * @param element the element that is added as String
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createORSetAdd(String element) {
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
    public static AntidoteMapUpdate createORSetAdd(List<String> elementList) {
        return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.ORSetType, AntidoteSetOpType.SetAdd);
    }

    /**
     * Creates the OR-Set remove.
     *
     * @param element the element that is removed as String
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createORSetRemove(String element) {
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
    public static AntidoteMapUpdate createORSetRemove(List<String> elementList) {
        return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.ORSetType, AntidoteSetOpType.SetRemove);

    }

    /**
     * Creates the RW-Set add.
     *
     * @param element the element that is added as String
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createRWSetAdd(String element) {
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
    public static AntidoteMapUpdate createRWSetAdd(List<String> elementList) {
        return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.RWSetType, AntidoteSetOpType.SetAdd);
    }

    /**
     * Creates the RW-Set remove.
     *
     * @param element the element that is removed as String
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createRWSetRemove(String element) {
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
    public static AntidoteMapUpdate createRWSetRemove(List<String> elementList) {
        return createSetRemoveHelper(stringListToBSList(elementList), AntidoteType.RWSetType, AntidoteSetOpType.SetRemove);
    }

    /**
     * Creates the set remove helper.
     *
     * @param elementList the element list
     * @param type        the type
     * @param opNumber    the operation number
     * @return the antidote map update
     */
    private static AntidoteMapUpdate createSetRemoveHelper(List<ByteString> elementList, CRDT_type type, int opNumber) {
        ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
        ApbSetUpdate.Builder upBuilder = ApbSetUpdate.newBuilder();
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(opNumber);
        if (opNumber == AntidoteSetOpType.SetRemove) {
            for (ByteString element : elementList) {
                upBuilder.addRems(element);
            }
        } else if (opNumber == AntidoteSetOpType.SetAdd) {
            for (ByteString element : elementList) {
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
    public static AntidoteMapUpdate createRegisterSet(ByteString value) {
        return createRegisterSetHelper(value, AntidoteType.LWWRegisterType);
    }

    /**
     * Creates the MV-Register set.
     *
     * @param value the value to which the MV-Register is set
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMVRegisterSet(ByteString value) {
        return createRegisterSetHelper(value, AntidoteType.MVRegisterType);
    }

    /**
     * Creates the register set.
     *
     * @param value the value to which the register is set
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createRegisterSet(String value) {
        return createRegisterSetHelper(ByteString.copyFromUtf8(value), AntidoteType.LWWRegisterType);
    }

    /**
     * Creates the MV-Register set.
     *
     * @param value the value to which the MV-Register is set
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMVRegisterSet(String value) {
        return createRegisterSetHelper(ByteString.copyFromUtf8(value), AntidoteType.MVRegisterType);
    }

    /**
     * Creates the register set helper.
     *
     * @param value the value
     * @param type  the type
     * @return the antidote map update
     */
    private static AntidoteMapUpdate createRegisterSetHelper(ByteString value, CRDT_type type) {
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
     * @param key    the key of the entry to be updated
     * @param update the update which is executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createGMapUpdate(String key, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
        updateList.add(update);
        return createGMapUpdate(key, updateList);
    }

    /**
     * Creates the G-Map update.
     *
     * @param key        the key of the entry to be updated
     * @param updateList the list of updates which are executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createGMapUpdate(String key, List<AntidoteMapUpdate> updateList) {
        return createMapUpdateHelper(ByteString.copyFromUtf8(key), updateList, AntidoteType.GMapType);
    }

    /**
     * Creates the AW-Map update.
     *
     * @param key    the key of the entry to be updated
     * @param update the update which is executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createAWMapUpdate(String key, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
        updateList.add(update);
        return createAWMapUpdate(key, updateList);
    }

    /**
     * Creates the AW-Map update.
     *
     * @param key        the key
     * @param updateList the list of updates which are executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createAWMapUpdate(String key, List<AntidoteMapUpdate> updateList) {
        return createMapUpdateHelper(ByteString.copyFromUtf8(key), updateList, AntidoteType.AWMapType);
    }

    /**
     * Creates the G-Map update.
     *
     * @param key    the key of the entry to be updated
     * @param update the update which is executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createGMapUpdateBS(ByteString key, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
        updateList.add(update);
        return createGMapUpdateBS(key, updateList);
    }

    /**
     * Creates the G-Map update.
     *
     * @param key        the key of the entry to be updated
     * @param updateList the list of updates which are executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createGMapUpdateBS(ByteString key, List<AntidoteMapUpdate> updateList) {
        return createMapUpdateHelper(key, updateList, AntidoteType.GMapType);
    }

    /**
     * Creates the AW-Map update.
     *
     * @param key    the key of the entry to be updated
     * @param update the update which is executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createAWMapUpdateBS(ByteString key, AntidoteMapUpdate update) {
        List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
        updateList.add(update);
        return createAWMapUpdateBS(key, updateList);
    }

    /**
     * Creates the AW-Map update.
     *
     * @param key        the key
     * @param updateList the list of updates which are executed on a particular entry of the map
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createAWMapUpdateBS(ByteString key, List<AntidoteMapUpdate> updateList) {
        return createMapUpdateHelper(key, updateList, AntidoteType.AWMapType);
    }

    /**
     * Creates the map update helper.
     *
     * @param key        the key
     * @param updateList the update list
     * @param mapType    the map type
     * @return the antidote map update
     */
    private static AntidoteMapUpdate createMapUpdateHelper(ByteString key, List<AntidoteMapUpdate> updateList, CRDT_type mapType) {
        CRDT_type type = updateList.get(0).getType();
        List<ApbUpdateOperation> apbUpdateList = new ArrayList<ApbUpdateOperation>();
        for (AntidoteMapUpdate u : updateList) {
            if (!(type.equals(u.getType()))) {
                throw new IllegalArgumentException("Different types detected, only one type allowed");
            }
            apbUpdateList.add(u.getOperation());
        }
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setKey(key);
        apbKeyBuilder.setType(type);
        ApbMapKey apbKey = apbKeyBuilder.build();
        ApbUpdateOperation.Builder opBuilder = ApbUpdateOperation.newBuilder();
        ApbMapUpdate.Builder upBuilder = ApbMapUpdate.newBuilder();

        ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder();
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        for (ApbUpdateOperation update : apbUpdateList) {
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
     * @param key  the key
     * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMapRemove(String key, CRDT_type type) {
        List<String> keyList = new ArrayList<>();
        keyList.add(key);
        return createMapRemove(keyList, type);
    }

    /**
     * Creates the map remove.
     *
     * @param keyList the key list
     * @param type    the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMapRemove(List<String> keyList, CRDT_type type) {
        List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setType(type);
        for (String key : keyList) {
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
     * Creates the map remove.
     *
     * @param key  the key
     * @param type the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMapRemoveBS(ByteString key, CRDT_type type) {
        List<ByteString> keyList = new ArrayList<>();
        keyList.add(key);
        return createMapRemoveBS(keyList, type);
    }

    /**
     * Creates the map remove.
     *
     * @param keyList the key list
     * @param type    the type, use AntidoteType._Type in the method call (AntidoteType.CounterType for example)
     * @return the antidote map update
     */
    public static AntidoteMapUpdate createMapRemoveBS(List<ByteString> keyList, CRDT_type type) {
        List<ApbMapKey> apbKeyList = new ArrayList<ApbMapKey>();
        ApbMapKey.Builder keyBuilder = ApbMapKey.newBuilder();
        keyBuilder.setType(type);
        for (ByteString key : keyList) {
            keyBuilder.setKey(key);
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
    private static List<ByteString> stringListToBSList(List<String> elementList) {
        List<ByteString> bsElementList = new ArrayList<>();
        for (String element : elementList) {
            bsElementList.add(ByteString.copyFromUtf8(element));
        }
        return bsElementList;
    }
}
