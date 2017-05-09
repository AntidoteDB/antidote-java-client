package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;

public class Bucket implements CrdtContainer {
    private final ByteString name;

    public Bucket(ByteString name) {
        this.name = name;
    }

    public ByteString getName() {
        return name;
    }

    @Override
    public AntidotePB.ApbReadObjectResp read(InteractiveTransaction tx, AntidotePB.CRDT_type type, ByteString key) {
        // TODO implement this
        throw new RuntimeException("TODO implement");
    }

    @Override
    public void update(AntidoteTransaction tx, AntidotePB.CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder) {
        // TODO implement this
        throw new RuntimeException("TODO implement");
    }
}
