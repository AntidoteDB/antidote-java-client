package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

import java.util.OptionalLong;

/**
 * The Class AntidoteOuterInteger.
 */
public final class CrdtInteger extends AntidoteCRDT {

    private static final CrdtCreator<CrdtInteger> CREATOR = new CrdtCreator<CrdtInteger>() {
        @Override
        public AntidotePB.CRDT_type type() {
            return AntidotePB.CRDT_type.INTEGER;
        }

        @Override
        public CrdtInteger create(CrdtContainer c, ByteString key) {
            return c.integer(key).createAntidoteInteger();
        }

        @Override
        public CrdtInteger cast(AntidoteCRDT value) {
            return (CrdtInteger) value;
        }
    };
    /**
     * The value of the integer.
     */
    private long value;

    private long delta;

    private OptionalLong assigned = OptionalLong.empty();


    /**
     * The low level integer.
     */
    private final IntegerRef ref;

    /**
     * Instantiates a new antidote integer.
     */
    CrdtInteger(IntegerRef ref) {
        this.ref = ref;
    }

    /**
     * Gets the value.
     */
    public long getValue() {
        return value;
    }

    /**
     * Sets the value.
     */
    public void set(int newValue) {
        value = newValue;
        assigned = OptionalLong.of(newValue);
        delta = 0;
    }


    /**
     * Increment by one.
     */
    public void increment() {
        increment(1);
    }

    /**
     * Increment by inc.
     *
     * @param inc the value by which the integer is incremented
     */
    public void increment(int inc) {
        value += inc;
        delta += inc;
    }

    @Override
    public IntegerRef getRef() {
        return ref;
    }

    @Override
    void updateFromReadResponse(AntidotePB.ApbReadObjectResp object) {
        value = object.getInt().getValue();
        assigned = OptionalLong.empty();
        delta = 0;

    }

    @Override
    public void push(AntidoteTransaction tx) {
        if (assigned.isPresent()) {
            ref.set(tx, assigned.getAsLong());
        }
        if (delta != 0) {
            ref.increment(tx, delta);
        }
        assigned = OptionalLong.empty();
        delta = 0;
    }

    public static CrdtCreator<CrdtInteger> creator() {
        return CREATOR;
    }
}
