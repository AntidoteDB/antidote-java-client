package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import eu.antidotedb.client.crdt.GMapCRDT;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteInnerGMap.
 */
public final class AntidoteInnerGMap extends AntidoteInnerMap implements GMapCRDT {

    /**
     * Instantiates a new antidote map G map entry.
     *
     * @param entryList      the entry list
     * @param antidoteClient the antidote client
     * @param name           the name
     * @param bucket         the bucket
     * @param path           the path
     * @param outerMapType   the outer map type
     */
    public AntidoteInnerGMap(List<AntidoteInnerCRDT> entryList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
        super(entryList, antidoteClient, name, bucket, path, outerMapType);
    }

    /**
     * Update the entry with the given key.
     *
     * @param mapKey              the map key
     * @param update              the update
     * @param antidoteTransaction the antidote transaction
     */
    public void update(String mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction) {
        updateBS(ByteString.copyFromUtf8(mapKey), update, antidoteTransaction);
    }

    /**
     * Update the entry with the given key with multiple updates.
     *
     * @param mapKey              the map key
     * @param updateList          the update list
     * @param antidoteTransaction the antidote transaction
     */
    public void update(String mapKey, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction) {
        updateBS(ByteString.copyFromUtf8(mapKey), updateList, antidoteTransaction);
    }

    /**
     * Update the entry with the given key.
     *
     * @param mapKey              the map key
     * @param update              the update
     * @param antidoteTransaction the antidote transaction
     */
    public void updateBS(ByteString mapKey, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction) {
        List<AntidoteMapUpdate> updateList = new ArrayList<AntidoteMapUpdate>();
        updateList.add(update);
        updateBS(mapKey, updateList, antidoteTransaction);
    }

    /**
     * Update the entry with the given key with multiple updates.
     *
     * @param mapKey              the map key
     * @param updateList          the update list
     * @param antidoteTransaction the antidote transaction
     */
    public void updateBS(ByteString mapKey, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction) {
        updateLocal(mapKey, updateList);
        List<AntidoteMapUpdate> innerMapUpdate = new ArrayList<AntidoteMapUpdate>();
        innerMapUpdate.add(AntidoteMapUpdate.createGMapUpdateBS(mapKey, updateList));
        updateHelper(innerMapUpdate, antidoteTransaction);
    }
}
