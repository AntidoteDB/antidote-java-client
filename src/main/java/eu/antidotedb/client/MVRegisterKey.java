package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.List;

public class MVRegisterKey<T> extends Key<List<T>> {
    private final ValueCoder<T> format;

    public MVRegisterKey(ByteString key, ValueCoder<T> format) {
        super(AntidotePB.CRDT_type.MVREG, key);
        this.format = format;
    }

    @CheckReturnValue
    public InnerUpdateOpImpl assign(T value) {
        return RegisterKey.buildRegisterUpdate(this, format.encode(value));
    }


    @Override
    List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.multiValueRegister(format).readResponseToValue(resp);
    }
}
