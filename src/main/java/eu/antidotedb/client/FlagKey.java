package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;

public class FlagKey extends Key<Boolean> {

    FlagKey(AntidotePB.CRDT_type type, ByteString key) {
        super(type, key);
    }

    @Override
    Boolean readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.flag().readResponseToValue(resp);
    }

    /**
     * Creates an update operation which assigns a new value to the flag.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOpDefaultImpl assign(boolean value) {
        AntidotePB.ApbFlagUpdate.Builder flagUpdateInstruction = AntidotePB.ApbFlagUpdate.newBuilder(); // The specific instruction in update instructions
        flagUpdateInstruction.setValue(value);
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setFlagop(flagUpdateInstruction);
        return new UpdateOpDefaultImpl(this, updateOperation);
    }
}
