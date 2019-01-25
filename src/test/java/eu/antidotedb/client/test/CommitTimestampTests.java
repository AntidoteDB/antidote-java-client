package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class CommitTimestampTests extends AbstractAntidoteTest {


    @Test
    public void timestampsInteractive() {
        MVRegisterKey<String> reg = Key.multiValueRegister("timestampsInteractive", ValueCoder.utf8String);
        CommitInfo tx1Info;
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            bucket.update(tx, reg.assign("Abc"));
            tx1Info = tx.commitTransaction();
        }
        try (InteractiveTransaction tx = antidoteClient.startTransaction(tx1Info)) {
            assertEquals(singletonList("Abc"), bucket.read(tx, reg));
            bucket.update(tx, reg.assign("xyz"));
            assertEquals(singletonList("xyz"), bucket.read(tx, reg));
            tx.commitTransaction();
        }
    }


    @Test
    public void timestampsNoTx() {
        MVRegisterKey<String> reg = Key.multiValueRegister("timestampsNoTx", ValueCoder.utf8String);
        NoTransaction tx = antidoteClient.noTransaction();
        bucket.update(tx, reg.assign("Abc"));
        tx = antidoteClient.noTransaction(tx.getLastCommitTimestamp());
        assertEquals(singletonList("Abc"), bucket.read(tx, reg));
    }

    @Test
    public void timestampsStaticTx() {
        MVRegisterKey<String> reg = Key.multiValueRegister("timestampsStaticTx", ValueCoder.utf8String);
        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        bucket.update(tx, reg.assign("Abc"));
        CommitInfo commitInfo = tx.commitTransaction();
        tx = antidoteClient.createStaticTransaction(commitInfo);
        bucket.update(tx, reg.assign("xyz"));
        commitInfo = tx.commitTransaction();
        assertEquals(singletonList("xyz"), bucket.read(antidoteClient.noTransaction(commitInfo), reg));
    }

    @Test
    public void timestampsNoTxLoop() {
        MVRegisterKey<Integer> reg = Key.multiValueRegister("timestampsNoTxLoop", ValueCoder.integerCoder);
        NoTransaction tx = antidoteClient.noTransaction();
        for (int i = 0; i < 1000; i++) {
            bucket.update(tx, reg.assign(i));
            tx = antidoteClient.noTransaction(tx.getLastCommitTimestamp());
            assertEquals(singletonList(i), bucket.read(tx, reg));
        }
    }
}
