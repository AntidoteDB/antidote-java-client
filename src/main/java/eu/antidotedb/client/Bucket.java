package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A bucket represents a section of the database.
 * Use the {@link #bucket(String)} or {@link #bucket(ByteString)} method to get a Bucket reference.
 * <p>
 * The bucket provides methods to execute reads and writes in the context of the bucket.
 */
public class Bucket {
    private final ByteString name;

    Bucket(ByteString name) {
        this.name = name;
    }

    /**
     * Get the bucket with the given name.
     */
    public static Bucket bucket(String name) {
        return new Bucket(ByteString.copyFromUtf8(name));
    }

    /**
     * Get the bucket with the given name.
     */
    public static Bucket bucket(ByteString name) {
        return new Bucket(name);
    }


    /**
     * @return the name of the bucket
     */
    public ByteString getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Bucket-" + name;
    }

    /**
     * Reads one object from the database.
     *
     * @param tx  The transaction context in which the read will be executed
     * @param key The key of the object to read
     * @param <T> The type of the value being read
     * @return the returned value
     * @throws AntidoteException when there is a problem with the database
     */
    public <T> T read(TransactionWithReads tx, Key<T> key) {
        AntidotePB.ApbReadObjectsResp resp = tx.readHelper(name, key.getKey(), key.getType());
        if (resp.getSuccess()) {
            return key.readResponseToValue(resp.getObjects(0));
        } else {
            throw new AntidoteException("Error when reading " + key + " (error code " + resp.getErrorcode() + ")");
        }
    }


    /**
     * Reads several objects from the database.
     *
     * @param tx   The transaction context in which the read will be executed
     * @param keys The keys of the objects to read
     * @param <T>  The type of the values being read
     * @return the returned values
     * @throws AntidoteException when there is a problem with the database
     */
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


    /**
     * Reads one object from the database in a batch transaction.
     *
     * @param tx  The transaction context in which the read will be executed
     * @param key The key of the object to read
     * @param <T> The type of the value being read
     * @return an object which will contain the read value after the batch transaction was committed.
     * @throws AntidoteException when there is a problem with the database
     */
    public <T> BatchReadResult<T> read(BatchRead tx, Key<T> key) {
        BatchReadResult<AntidotePB.ApbReadObjectResp> resp = tx.readHelper(name, key.getKey(), key.getType());
        return resp.map(key::readResponseToValue);
    }

    /**
     * Performs an update on the database.
     *
     * @param tx     The transaction context in which the update will be executed.
     * @param update The update operation to perform. Use the {@link Key} class to create update operations on a key.
     * @throws AntidoteException when there is a problem with the database
     */
    public void update(AntidoteTransaction tx, UpdateOp update) {
        update(tx, Collections.singleton(update));
    }

    /**
     * Performs several updates on the database.
     *
     * @param tx      The transaction context in which the update will be executed.
     * @param updates The update operations to perform. Use the {@link Key} class to create update operations on a key.
     * @throws AntidoteException when there is a problem with the database
     */
    public void update(AntidoteTransaction tx, Collection<? extends UpdateOp> updates) {
        tx.performUpdates(updates.stream().map(upd -> upd.getApbUpdate(name)).collect(Collectors.toList()));
    }


}
