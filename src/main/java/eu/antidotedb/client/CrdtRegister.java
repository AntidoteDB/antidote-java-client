package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

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

    public static <V> CrdtCreator<CrdtRegister<V>> creator(ValueCoder<V> valueCoder) {
        return new CrdtCreator<CrdtRegister<V>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.LWWREG;
            }

            @Override
            public <K> CrdtRegister<V> create(CrdtContainer<K> c, K key) {
                return c.register(key, valueCoder).createAntidoteLWWRegister();
            }

            @Override
            public CrdtRegister<V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return (CrdtRegister<V>) value;
            }
        };
    }
}
