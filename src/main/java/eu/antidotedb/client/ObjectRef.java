package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelObject.
 */
public abstract class ObjectRef<Value> {
    private final CrdtContainer<?> container;

    private final ByteString key;

    private CRDT_type type;

    /**
     * Instantiates a new reference to a crdt
     */
    ObjectRef(CrdtContainer<?> container, ByteString key, CRDT_type type) {
        this.key = key;
        this.container = container;
        this.type = type;
    }

    public CRDT_type getType() {
        return type;
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public ByteString getKey() {
        return key;
    }


    /**
     * Gets the client.
     *
     * @return the client
     */
    public CrdtContainer<?> getContainer() {
        return container;
    }


    /**
     * Reads the current value of this object from the database
     */
    public final Value read(TransactionWithReads tx) {
        AntidotePB.ApbReadObjectResp resp = getContainer().read(tx, getType(), getKey());
        return readResponseToValue(resp);
    }

    public final BatchReadResult<Value> read(BatchRead tx) {
        BatchReadResult<AntidotePB.ApbReadObjectResp> resp = getContainer().readBatch(tx, getType(), getKey());
        return resp.map(this::readResponseToValue);
    }

    abstract Value readResponseToValue(AntidotePB.ApbReadObjectResp resp);


    AntidotePB.ApbReadObjectResp readValue(TransactionWithReads tx) {
        return container.read(tx, type, key);
    }

    BatchReadResult<AntidotePB.ApbReadObjectResp> readValue(BatchRead tx) {
        return container.readBatch(tx, type, key);
    }



    @Override
    public String toString() {
        return container + "/" + type + "_" + key;
    }


}
