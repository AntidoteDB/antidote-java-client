package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.crdt.SetCRDT;

import java.util.List;

/**
 * The Class AntidoteOuterORSet.
 */
public final class AntidoteOuterORSet extends AntidoteOuterSet implements SetCRDT {

    /**
     * The low level set.
     */
    private final ORSetRef lowLevelSet;

    /**
     * Instantiates a new antidote OR set.
     *
     * @param name           the name
     * @param bucket         the bucket
     * @param valueList      the value list
     * @param antidoteClient the antidote client
     */
    public AntidoteOuterORSet(String name, String bucket, List<ByteString> valueList, AntidoteClient antidoteClient) {
        super(name, bucket, valueList, antidoteClient, AntidoteType.ORSetType);
        lowLevelSet = new ORSetRef(name, bucket, antidoteClient);
    }

    /**
     * Gets the most recent state from the database.
     */
    public void readDatabase(AntidoteTransaction antidoteTransaction) {
        setValues(lowLevelSet.readValueListBS(antidoteTransaction));
    }

    /**
     * Gets the most recent state from the database.
     */
    public void readDatabase() {
        setValues(lowLevelSet.readValueListBS());
    }

    protected void readValueList(List<ByteString> newValueList) {
        setValues(newValueList);
    }
}
