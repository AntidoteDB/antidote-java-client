package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import eu.antidotedb.client.crdt.LWWRegisterCRDT;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteInnerLWWRegister.
 */
public final class AntidoteInnerLWWRegister extends AntidoteInnerCRDT implements LWWRegisterCRDT {

    /**
     * The value.
     */
    private ByteString value;

    /**
     * Instantiates a new antidote map register entry.
     *
     * @param value          the value
     * @param antidoteClient the antidote client
     * @param name           the name
     * @param bucket         the bucket
     * @param path           the path
     * @param outerMapType   the outer map type
     */
    public AntidoteInnerLWWRegister(String value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
        super(antidoteClient, name, bucket, path, outerMapType);
        this.value = ByteString.copyFromUtf8(value);
    }

    /**
     * Instantiates a new antidote map register entry.
     *
     * @param value          the value
     * @param antidoteClient the antidote client
     * @param name           the name
     * @param bucket         the bucket
     * @param path           the path
     * @param outerMapType   the outer map type
     */
    public AntidoteInnerLWWRegister(ByteString value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
        super(antidoteClient, name, bucket, path, outerMapType);
        this.value = value;
    }

    /**
     * Gets the most recent state from the database.
     */
    public void readDatabase(AntidoteTransaction antidoteTransaction) {
        AntidoteInnerLWWRegister register;
        if (getType() == AntidoteType.GMapType) {
            GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
            AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap(antidoteTransaction);
            if (getPath().size() == 1) {
                register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
            } else {
                register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size() - 1).getKey().toStringUtf8());
            }
            value = register.getValueBS();
        } else if (getType() == AntidoteType.AWMapType) {
            AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
            AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
            if (getPath().size() == 1) {
                register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
            } else {
                register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size() - 1).getKey().toStringUtf8());
            }
            value = register.getValueBS();
        }
    }

    /**
     * Gets the most recent state from the database.
     */
    public void readDatabase() {
        AntidoteInnerLWWRegister register;
        if (getType() == AntidoteType.GMapType) {
            GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
            AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
            if (getPath().size() == 1) {
                register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
            } else {
                register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size() - 1).getKey().toStringUtf8());
            }
            value = register.getValueBS();
        } else if (getType() == AntidoteType.AWMapType) {
            AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
            AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
            if (getPath().size() == 1) {
                register = outerMap.getLWWRegisterEntry(getPath().get(0).getKey().toStringUtf8());
            } else {
                register = readDatabaseHelper(getPath(), outerMap).getLWWRegisterEntry(getPath().get(getPath().size() - 1).getKey().toStringUtf8());
            }
            value = register.getValueBS();
        }
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public String getValue() {
        return value.toStringUtf8();
    }

    /* (non-Javadoc)
     * @see eu.antidotedb.client.RegisterInterface#getValueBS()
     */
    public ByteString getValueBS() {
        return value;
    }

    /**
     * Locally set the register to a new value.
     *
     * @param value the value
     */
    protected void setLocal(ByteString value) {
        this.value = value;
    }

    /**
     * Set the register to a new value.
     *
     * @param value               the value
     * @param antidoteTransaction the antidote transaction
     */
    public void setValue(String value, AntidoteTransaction antidoteTransaction) {
        setLocal(ByteString.copyFromUtf8(value));
        List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>();
        registerSet.add(AntidoteMapUpdate.createRegisterSet(value));
        updateHelper(registerSet, antidoteTransaction);
    }

    public void setValueBS(ByteString value, AntidoteTransaction antidoteTransaction) {
        setLocal(value);
        List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>();
        registerSet.add(AntidoteMapUpdate.createRegisterSet(value.toStringUtf8()));
        updateHelper(registerSet, antidoteTransaction);
    }
}
