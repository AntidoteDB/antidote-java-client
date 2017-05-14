package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbReadObjectResp;
import com.google.protobuf.ByteString;

public class Bucket<Key> implements CrdtContainer<Key> {
    private final ByteString name;
    private final ValueCoder<Key> keyCoder;


    Bucket(ByteString name, ValueCoder<Key> keyCoder) {
        this.name = name;
        this.keyCoder = keyCoder;
    }

    public static Bucket<String> create(String name) {
        return new Bucket<>(ByteString.copyFromUtf8(name), ValueCoder.utf8String);
    }

    public static <K> Bucket<K> create(String name, ValueCoder<K> keyCoder) {
        return new Bucket<>(ByteString.copyFromUtf8(name), keyCoder);
    }

    public static <K> Bucket<K> create(ByteString name, ValueCoder<K> keyCoder) {
        return new Bucket<>(name, keyCoder);
    }


    public ByteString getName() {
        return name;
    }


    @Override
    public ApbReadObjectResp read(TransactionWithReads tx, AntidotePB.CRDT_type type, ByteString key) {
        AntidotePB.ApbReadObjectsResp apbReadObjectsResp = tx.readHelper(name, key, type);
        return apbReadObjectsResp.getObjects(0);
    }

    @Override
    public BatchReadResult<ApbReadObjectResp> readBatch(BatchRead tx, AntidotePB.CRDT_type type, ByteString key) {
        return tx.readHelper(name, key, type);
    }

    @Override
    public void update(AntidoteTransaction tx, AntidotePB.CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder) {
        AntidotePB.ApbUpdateOp.Builder update = AntidotePB.ApbUpdateOp.newBuilder();
        AntidotePB.ApbBoundObject.Builder boundObject = AntidotePB.ApbBoundObject.newBuilder();
        boundObject.setBucket(name);
        boundObject.setKey(key);
        boundObject.setType(type);
        update.setBoundobject(boundObject);
        update.setOperation(builder);
        tx.performUpdate(update);
    }

    @Override
    public ValueCoder<Key> keyCoder() {
        return keyCoder;
    }
}
