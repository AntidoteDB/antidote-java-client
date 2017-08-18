package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Bucket {
    private final ByteString name;

    Bucket(ByteString name) {
        this.name = name;
    }

    public static Bucket create(String name) {
        return new Bucket(ByteString.copyFromUtf8(name));
    }

    public static Bucket create(ByteString name) {
        return new Bucket(name);
    }


    public ByteString getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Bucket-" + name;
    }

    public <T> T read(TransactionWithReads tx, Key<T> key) {
        AntidotePB.ApbReadObjectsResp resp = tx.readHelper(name, key.getKey(), key.getType());
        if (resp.getSuccess()) {
            return key.readResponseToValue(resp.getObjects(0));
        } else {
            throw new AntidoteException("Error when reading " + key + " (error code " + resp.getErrorcode() + ")");
        }
    }


    public <T> List<T> readAll(TransactionWithReads tx, Collection<? extends Key<? extends T>> keys) {
        BatchRead batchRead = new BatchRead();
        List<BatchReadResult<? extends T>> results = new ArrayList<>(keys.size());
        for (Key<? extends T> key : keys) {
            BatchReadResult<? extends T> read = read(batchRead, key);
            results.add(read);
        }
        batchRead.commit(tx);
        return results.stream()
                .map(BatchReadResult::get)
                .collect(Collectors.toList());
    }


    public <T> BatchReadResult<T> read(BatchRead tx, Key<T> key) {
        BatchReadResult<AntidotePB.ApbReadObjectResp> resp = tx.readHelper(name, key.getKey(), key.getType());
        return resp.map(key::readResponseToValue);
    }

    public void update(AntidoteTransaction tx, InnerUpdateOp update) {
        update(tx, Collections.singleton(update));
    }

    public void update(AntidoteTransaction tx, Collection<? extends InnerUpdateOp> updates) {
        tx.performUpdates(updates.stream().map(this::makeUpd).collect(Collectors.toList()));
    }

    private AntidotePB.ApbUpdateOp.Builder makeUpd(InnerUpdateOp upd) {
        return upd.getApbUpdate(name);
    }


}
