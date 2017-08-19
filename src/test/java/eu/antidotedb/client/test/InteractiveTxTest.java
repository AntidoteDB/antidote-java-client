package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class InteractiveTxTest extends AbstractAntidoteTest {

    @Test
    public void testEmptyTx() {
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
        }
    }

    @Test
    public void interactiveExample() {
        ValueCoder<Integer> intCoder = ValueCoder.stringCoder(Object::toString, Integer::valueOf);
        CounterKey c = Key.counter("my_example_counter");
        SetKey<Integer> numberSet = Key.set("set_of_numbers", intCoder);
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            int val = bucket.read(tx, c);
            bucket.update(tx, numberSet.add(val));
        }
    }

    @Test
    public void testInteractiveTx() {
        RegisterKey<String> reg = Key.register("testInteractiveTx_reg1", ValueCoder.utf8String);
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            bucket.update(tx, reg.assign("Abc"));
            tx.commitTransaction();
        }
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            assertEquals("Abc", bucket.read(tx, reg));
            bucket.update(tx, reg.assign("xyz"));
            assertEquals("xyz", bucket.read(tx, reg));
            tx.commitTransaction();
        }
    }

    @Test
    public void testAbort() {
        RegisterKey<String> reg = Key.register("testAbort", ValueCoder.utf8String);
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            bucket.update(tx, reg.assign("Abc"));
            tx.abortTransaction();
        }
        String x = bucket.read(antidoteClient.noTransaction(), reg);
        assertEquals("", x);
    }


    @Test
    public void testMany() {
        IntStream.range(0, 100).parallel().forEach(i -> {
            RegisterKey<String> reg = Key.register("testInteractiveTx_reg" + i, ValueCoder.utf8String);
            try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
                bucket.update(tx, reg.assign("" + i));
                tx.commitTransaction();
            }
        });
        RegisterKey<String> reg = Key.register("testInteractiveTx_reg99", ValueCoder.utf8String);

        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            assertEquals("99", bucket.read(tx, reg));
            tx.commitTransaction();
        }
    }

    @Test
    public void testManyBatch() {
        RegisterKey<String> reg1 = Key.register("manyBatch_reg1", ValueCoder.utf8String);
        RegisterKey<String> reg2 = Key.register("manyBatch_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        bucket.update(stx, reg1.assign("a"));
        bucket.update(stx, reg2.assign("b"));
        stx.commitTransaction();

        IntStream.range(0, 100).forEach(i -> {
            try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
                BatchRead br = antidoteClient.newBatchRead();
                BatchReadResult<String> v1 = bucket.read(br, reg1);
                BatchReadResult<String> v2 = bucket.read(br, reg2);
                br.commit(tx);
                tx.commitTransaction();
            }
        });
    }
}
