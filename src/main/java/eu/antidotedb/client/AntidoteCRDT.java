package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

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
}
