package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * The Class LowLevelObject.
 */
public abstract class ObjectRef {
    private final CrdtContainer container;

    private final ByteString key;

    private CRDT_type type;

    /**
     * Instantiates a new reference to a crdt
     */
    ObjectRef(CrdtContainer container, ByteString key, CRDT_type type) {
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
    public CrdtContainer getContainer() {
        return container;
    }


    /**
     * Reads the current value of this object from the database
     */
    public abstract Object read(InteractiveTransaction tx);


    AntidotePB.ApbReadObjectResp readValue(InteractiveTransaction tx) {
        return container.read(tx, type, key);
    }



    @Override
    public String toString() {
        return container + "/" + type + "_" + key;
    }
}
