package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;

/**
 * The Class AntidoteObject.
 */
public abstract class AntidoteCRDT {


    /**
     * The immutable reference to the unerlying database object
     */
    public abstract ObjectRef getRef();

    /**
     * update the internal state of this CRDT from a read response
     */
    public abstract void updateFromReadResponse(AntidotePB.ApbReadObjectResp object);

    /**
     * pull in changes from the database to update the state of this object.
     * Uses a BatchRead to collect several pull-requests and executes them in one request.
     * The effect is only visible after the BatchRead is committed.
     */
    public abstract void pull(BatchRead batchRead);

    /**
     * pull in changes from the database to update the state of this object.
     */
    public abstract void pull(TransactionWithReads tx);
}
