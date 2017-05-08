package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteObject.
 */
public abstract class AntidoteCRDT {

    /**
     * A reference to the underlying database object
     */
    private final ObjectRef objectRef;

    /**
     * Instantiates a new antidote object.
     *
     */
    AntidoteCRDT(ObjectRef objectRef) {
        this.objectRef = objectRef;
    }

    /**
     * The immutable reference to the unerlying database object
     */
    public ObjectRef getObjectRef() {
        return objectRef;
    }

    /**
     * update the internal state of this CRDT from a read response
     */
    public abstract void updateFromReadResponse(AntidotePB.ApbReadObjectResp object);
}
