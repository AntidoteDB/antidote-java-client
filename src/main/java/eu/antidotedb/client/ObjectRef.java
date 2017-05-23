package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOperation;
import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;

/**
 * An ObjectRef is an immutable reference to an object stored in the database.
 * The ObjectRef is independent from a specific connection, so it is possible to store ObjectRefs in constants.
 * <p>
 * ObjectRefs are typically created using the methods defined in {@link CrdtCreator}.
 * The most important instance of {@link CrdtCreator} is a {@link Bucket}.
 * <p>
 * Each ObjectRef has a {@link #read(BatchRead)} and {@link #read(BatchRead)} method, which are used to retrieve the current value of the object from the database.
 * The return type depends on the type parameter.
 * <p>
 * For performing operations each concrete subclass has type-specific methods.
 *
 * @param <Value> the type of the value stored in this object
 */
public abstract class ObjectRef<Value> extends ResponseDecoder<Value> {
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
     * @return The container in which this object is stored
     */
    public CrdtContainer<?> getContainer() {
        return container;
    }

    /**
     * Resets the value of this object
     */
    public void reset(AntidoteTransaction tx) {
        ApbUpdateOperation.Builder resetOp = ApbUpdateOperation.newBuilder();
        resetOp.setResetop(AntidotePB.ApbCrdtReset.newBuilder().build());
        getContainer().update(tx, getType(), getKey(), resetOp);
    }


    /**
     * Reads the current value of this object from the database
     *
     * @param tx A context for executing the read in. See {@link AntidoteClient#startTransaction()} and {@link AntidoteClient#noTransaction()}.
     */
    public final Value read(TransactionWithReads tx) {
        AntidotePB.ApbReadObjectResp resp = getContainer().read(tx, getType(), getKey());
        return readResponseToValue(resp);
    }

    /**
     * Reads the current value of this object from the database in a batch read.
     * The read-request is not executed immediately.
     * Instead a {@link BatchReadResult} is returned, which is a kind of Future from which the value can be returned after all reads in the batch have been performed.
     * <p>
     * Also see: {@link AntidoteClient#readObjects(TransactionWithReads, Iterable)}, which is easier to use for reading a collection of ObjectRefs with similar Value.
     *
     * @param tx A batch read (see {@link AntidoteClient#newBatchRead()})
     * @return A BatchReadResult which can be used to get the result later (see {@link BatchReadResult#get()})
     */
    public final BatchReadResult<Value> read(BatchRead tx) {
        BatchReadResult<AntidotePB.ApbReadObjectResp> resp = getContainer().readBatch(tx, getType(), getKey());
        return resp.map(this::readResponseToValue);
    }


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
