package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class LowLevelObject.
 */
public class ObjectRef {
    /**
     * The name.
     */
    private final String name;

    /**
     * The bucket.
     */
    private final String bucket;

    private CRDT_type type;

    /**
     * The antidote client.
     */
    private final AntidoteClient antidoteClient;

    /**
     * Instantiates a new low level object.
     *
     * @param name           the name
     * @param bucket         the bucket
     * @param antidoteClient the antidote client
     */
    public ObjectRef(String name, String bucket, AntidoteClient antidoteClient, CRDT_type type) {
        this.name = name;
        this.bucket = bucket;
        this.antidoteClient = antidoteClient;
        this.type = type;
    }

    public CRDT_type getType() {
        return type;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the bucket.
     *
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    ;

    /**
     * Gets the client.
     *
     * @return the client
     */
    public AntidoteClient getClient() {
        return antidoteClient;
    }

    /**
     * Gets the object value.
     *
     * @return the value
     */
    protected Object getObjectRefValue(ObjectRef objectRef) {
        List<ObjectRef> objectRefs = new ArrayList<>();
        objectRefs.add(objectRef);
        List<Object> objectRefValue = getClient().readObjects(objectRefs);
        Object object = objectRefValue.get(0);
        objectRefs.clear();
        return object;
    }

    @Override
    public String toString() {
        return "[" + type + " " + bucket + "/" + name + "]";
    }
}
