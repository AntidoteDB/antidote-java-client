package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

/**
 * The Class AntidoteObject.
 */
public abstract class AntidoteCRDT {


    /**
     * The immutable reference to the unerlying database object
     */
    public abstract ObjectRef<?> getRef();

    /**
     * update the internal state of this CRDT from a read response
     */
    abstract void updateFromReadResponse(AntidotePB.ApbReadObjectResp object);

    /**
     * pull in changes from the database to update the state of this object.
     * Uses a BatchRead to collect several pull-requests and executes them in one request.
     * The effect is only visible after the BatchRead is committed.
     */
    public void pull(BatchRead batchRead) {
        BatchReadResult<AntidotePB.ApbReadObjectResp> res = getRef().readValue(batchRead);
        res.whenReady(this::updateFromReadResponse);
    }

    /**
     * pull in changes from the database to update the state of this object.
     */
    public void pull(TransactionWithReads tx) {
        AntidotePB.ApbReadObjectResp response = getRef().readValue(tx);
        updateFromReadResponse(response);
    }

    public abstract void push(AntidoteTransaction tx);

}
