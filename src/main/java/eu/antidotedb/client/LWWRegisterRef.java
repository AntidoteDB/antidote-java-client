package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbGetRegResp;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelLWWRegister.
 */
public final class LWWRegisterRef extends RegisterRef {

    /**
     * Instantiates a new low level LWW register.
     *
     * @param name           the name
     * @param bucket         the bucket
     * @param antidoteClient the antidote client
     */
    public LWWRegisterRef(String name, String bucket, AntidoteClient antidoteClient) {
        super(name, bucket, antidoteClient, AntidoteType.LWWRegisterType);
    }

    /**
     * Sets the value.
     *
     * @param value               the value
     * @param antidoteTransaction the antidote transaction
     */
    public void setBS(ByteString value, AntidoteTransaction antidoteTransaction) {
        super.setBS(value, getType(), antidoteTransaction);
    }

    /**
     * Sets the value.
     *
     * @param value               the value
     * @param antidoteTransaction the antidote transaction
     */
    public void set(String value, AntidoteTransaction antidoteTransaction) {
        super.set(value, getType(), antidoteTransaction);
    }

    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote register
     */
    public AntidoteOuterLWWRegister createAntidoteLWWRegister(AntidoteTransaction antidoteTransaction) {
        ApbGetRegResp reg = antidoteTransaction.readHelper(this).getObjects(0).getReg();
        return new AntidoteOuterLWWRegister(getName(), getBucket(), reg.getValue(), getClient());
    }

    /**
     * Read register from database.
     *
     * @return the antidote register
     */
    public AntidoteOuterLWWRegister createAntidoteLWWRegister() {
        ByteString reg = (ByteString) getObjectRefValue(this);
        return new AntidoteOuterLWWRegister(getName(), getBucket(), reg, getClient());
    }

    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as ByteString
     */
    public ByteString readRegisterValueBS(AntidoteTransaction antidoteTransaction) {
        ApbGetRegResp reg = antidoteTransaction.readHelper(this).getObjects(0).getReg();
        return reg.getValue();
    }

    /**
     * Read register from database.
     *
     * @return the register value as ByteString
     */
    public ByteString readRegisterValueBS() {
        ByteString reg = (ByteString) getObjectRefValue(this);
        return reg;
    }

    /**
     * Read register from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the register value as String
     */
    public String readRegisterValue(AntidoteTransaction antidoteTransaction) {
        ApbGetRegResp reg = antidoteTransaction.readHelper(this).getObjects(0).getReg();
        return reg.getValue().toStringUtf8();
    }

    /**
     * Read register from database.
     *
     * @return the register value as String
     */
    public String readRegisterValue() {
        ByteString reg = (ByteString) getObjectRefValue(this);
        return reg.toStringUtf8();
    }
}
