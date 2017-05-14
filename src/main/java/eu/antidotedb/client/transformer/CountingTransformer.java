package eu.antidotedb.client.transformer;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.Connection;
import eu.antidotedb.client.messages.AntidoteResponse;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A Transformer, which counts how often each kind of operation has been executed
 */
public class CountingTransformer extends Transformer {
    private AtomicInteger readObjectsCounter = new AtomicInteger(0);
    private AtomicInteger updateObjectsCounter = new AtomicInteger(0);
    private AtomicInteger startTransactionCounter = new AtomicInteger(0);
    private AtomicInteger abortTransactionCounter = new AtomicInteger(0);
    private AtomicInteger commitTransactionCounter = new AtomicInteger(0);
    private AtomicInteger staticReadsCounter = new AtomicInteger(0);
    private AtomicInteger staticUpdatesCounter = new AtomicInteger(0);


    public int getReadObjectsCounter() {
        return readObjectsCounter.get();
    }

    public int getUpdateObjectsCounter() {
        return updateObjectsCounter.get();
    }

    public int getStartTransactionCounter() {
        return startTransactionCounter.get();
    }

    public int getAbortTransactionCounter() {
        return abortTransactionCounter.get();
    }

    public int getCommitTransactionCounter() {
        return commitTransactionCounter.get();
    }

    public int getStaticReadsCounter() {
        return staticReadsCounter.get();
    }

    public int getStaticUpdatesCounter() {
        return staticUpdatesCounter.get();
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbReadObjects op) {
        readObjectsCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbUpdateObjects op) {
        updateObjectsCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStartTransaction op) {
        startTransactionCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbAbortTransaction op) {
        abortTransactionCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbCommitTransaction op) {
        commitTransactionCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticReadObjects op) {
        staticReadsCounter.incrementAndGet();
        return super.handle(connection, op);
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticUpdateObjects op) {
        staticUpdatesCounter.incrementAndGet();
        return super.handle(connection, op);
    }
}
