package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;

/**
 * The Class AntidoteOuterLWWRegister.
 */
public final class CrdtRegister<T> extends AntidoteCRDT {

    /**
     * The value.
     */
    private T value;

    private boolean changed = false;

    /**
     * The low level register.
     */
    private final RegisterRef<T> ref;


    CrdtRegister(RegisterRef<T> lowLevelRegister) {
        this.ref = lowLevelRegister;
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public T getValue() {
        return value;
    }

    /**
     * Set the value of the register.
     *
     */
    public void set(T value) {
        this.value = value;
        this.changed = true;
    }

    @Override
    public RegisterRef<T> getRef() {
        return ref;
    }

    @Override
    void updateFromReadResponse(AntidotePB.ApbReadObjectResp object) {
        value = ref.getFormat().decode(object.getReg().getValue());
        changed = false;
    }

    @Override
    public void push(AntidoteTransaction tx) {
        if (changed) {
            ref.set(tx, value);
        }
        changed = false;
    }
}
