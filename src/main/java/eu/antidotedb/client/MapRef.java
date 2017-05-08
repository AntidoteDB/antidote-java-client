package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

/**
 * The Class LowLevelMap.
 */
public class MapRef extends ObjectRef implements CrdtFactory {

    /**
     * Instantiates a new low level map.
     *
     * @param name           the name
     * @param bucket         the bucket
     * @param antidoteClient the antidote client
     */
    public MapRef(String name, String bucket, AntidoteClient antidoteClient, CRDT_type type) {
        super(name, bucket, antidoteClient, type);
    }

    /**
     * Prepare the update operation builder.
     *
     * @param mapKey  the map key
     * @param updates the updates
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder updateOpBuilder(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates) {
        ApbMapNestedUpdate.Builder mapNestedUpdateBuilder = ApbMapNestedUpdate.newBuilder(); // The specific instruction in update instruction
        List<ApbMapNestedUpdate> mapNestedUpdateList = new ArrayList<ApbMapNestedUpdate>();
        ApbMapNestedUpdate mapNestedUpdate;
        for (AntidoteMapUpdate update : updates) {
            mapNestedUpdateBuilder.setUpdate(update.getOperation());
            mapNestedUpdateBuilder.setKey(mapKey.getApbKey());
            mapNestedUpdate = mapNestedUpdateBuilder.build();
            mapNestedUpdateList.add(mapNestedUpdate);
        }
        ApbMapUpdate.Builder mapUpdateInstruction = ApbMapUpdate.newBuilder(); // The specific instruction in update instruction
        mapUpdateInstruction.addAllUpdates(mapNestedUpdateList);

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdateInstruction);
        return updateOperation;
    }

    /**
     * Update.
     *
     * @param mapKey              the map key
     * @param update              the update
     * @param type                the type
     * @param antidoteTransaction the antidote transaction
     */
    public void update(AntidoteMapKey mapKey, AntidoteMapUpdate update, CRDT_type type, AntidoteTransaction antidoteTransaction) {
        List<AntidoteMapUpdate> updates = new ArrayList<>();
        updates.add(update);
        update(mapKey, updates, type, antidoteTransaction);
    }

    /**
     * Update.
     *
     * @param mapKey              the map key
     * @param updates             the updates
     * @param type                the type
     * @param antidoteTransaction the antidote transaction
     */
    public void update(AntidoteMapKey mapKey, List<AntidoteMapUpdate> updates, CRDT_type type, AntidoteTransaction antidoteTransaction) {
        antidoteTransaction.updateHelper(updateOpBuilder(mapKey, updates), getName(), getBucket(), type);
    }

//    /**
//     * Helper method for the common part of reading both kinds of maps.
//     *
//     * @param path         the path storing the key and type of all inner maps leading to the entry. This is needed when storing Map entries in variables
//     *                     that are a subclass of AntidoteMapEntry if we want to give them an update method.
//     * @param apbEntryList the ApbEntryList of the map, which is transformed into AntidoteMapEntries
//     * @param outerMapType the type of the outer Map (G-Map or AW-Map). The type of the outermost Map is not stored in the path
//     * @return the list of AntidoteMapEntries
//     */
//    protected List<AntidoteInnerCRDT> readMapHelper(List<ApbMapKey> path, List<ApbMapEntry> apbEntryList, CRDT_type outerMapType) {
//        List<AntidoteInnerCRDT> antidoteEntryList = new ArrayList<AntidoteInnerCRDT>();
//        path.add(null);
//        for (ApbMapEntry e : apbEntryList) {
//            path.set(path.size() - 1, e.getKey());
//            List<ApbMapKey> path2 = new ArrayList<ApbMapKey>();
//            switch (e.getKey().getType()) {
//                case COUNTER:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerCounter(
//                            e.getValue().getCounter().getValue(), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case ORSET:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerORSet(
//                            e.getValue().getSet().getValueList(), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case RWSET:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerRWSet(
//                            e.getValue().getSet().getValueList(), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case AWMAP:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerAWMap(
//                            readMapHelper(path, e.getValue().getMap().getEntriesList(), outerMapType), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case INTEGER:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerInteger(
//                            toIntExact(e.getValue().getInt().getValue()), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case LWWREG:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerLWWRegister(
//                            e.getValue().getReg().getValue(), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case MVREG:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerMVRegister(e.getValue().getMvreg().getValuesList(), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//                case GMAP:
//                    path2 = new ArrayList<ApbMapKey>();
//                    path2.addAll(path);
//                    antidoteEntryList.add(new AntidoteInnerGMap(
//                            readMapHelper(path, e.getValue().getMap().getEntriesList(), outerMapType), getClient(), getName(), getBucket(), path2, outerMapType));
//                    break;
//            }
//        }
//        path.remove(0);
//        return antidoteEntryList;
//    }
}
