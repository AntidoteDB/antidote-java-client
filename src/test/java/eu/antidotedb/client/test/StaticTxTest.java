package eu.antidotedb.client.test;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class StaticTxTest extends AbstractAntidoteTest {

    @Test
    public void batchReadExample() {
        CounterKey c1 = Key.counter("c1");
        CounterKey c2 = Key.counter("c2");
        CounterKey c3 = Key.counter("c3");

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        bucket.update(stx, c1.increment(100));
        bucket.update(stx, c2.increment(20));
        bucket.update(stx, c3.increment(3));
        stx.commitTransaction();

        BatchRead batchRead = antidoteClient.newBatchRead();
        BatchReadResult<Integer> c1val = bucket.read(batchRead, c1);
        BatchReadResult<Integer> c2val = bucket.read(batchRead, c2);
        BatchReadResult<Integer> c3val = bucket.read(batchRead, c3);
        batchRead.commit(antidoteClient.noTransaction());

        int sum = c1val.get() + c2val.get() + c3val.get();

        assertEquals(123, sum);

        List<Integer> values = bucket.readAll(antidoteClient.noTransaction(), Arrays.asList(c1, c2, c3));
        assertEquals(Arrays.asList(100,20,3), values);

    }

    @Test
    public void staticReadWrite() {
        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();

        RegisterKey<String> reg1 = Key.register("staticReadWrite_reg1", ValueCoder.utf8String);
        RegisterKey<String> reg2 = Key.register("staticReadWrite_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        bucket.update(stx, reg1.assign("a"));
        bucket.update(stx, reg2.assign("b"));
        stx.commitTransaction();

        BatchRead br = antidoteClient.newBatchRead();
        BatchReadResult<String> v1 = bucket.read(br, reg1);
        BatchReadResult<String> v2 = bucket.read(br, reg2);
        br.commit(antidoteClient.noTransaction());

        assertEquals("a", v1.get());
        assertEquals("b", v2.get());

        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }

    @Test
    public void staticReadWrite2() {
        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();

        RegisterKey<String> reg1 = Key.register("staticReadWrite2_reg1", ValueCoder.utf8String);
        RegisterKey<String> reg2 = Key.register("staticReadWrite2_reg2", ValueCoder.utf8String);

        bucket.updates(antidoteClient.noTransaction(),
                reg1.assign("a"),
                reg2.assign("b"));

        List<String> values = bucket.readAll(antidoteClient.noTransaction(), reg1, reg2);

        assertEquals("a", values.get(0));
        assertEquals("b", values.get(1));

        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }


    @Test
    public void staticReadWrite3() {

        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();

        RegisterKey<String> reg1 = Key.register("staticReadWrite_reg1", ValueCoder.utf8String);
        RegisterKey<String> reg2 = Key.register("staticReadWrite_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        bucket.update(stx, reg1.assign("a"));
        bucket.update(stx, reg2.assign("b"));
        stx.commitTransaction();

        class Obst {
            @Override
            public boolean equals(Object obj) {
                return obj.getClass().equals(this.getClass());
            }
        }
        class Apfel extends Obst {

        }
        class Birne extends Obst {

        }

        RegisterKey<Apfel> regA = Key.register("staticReadWrite_reg1", new ValueCoder<Apfel>() {
            @Override
            public ByteString encode(Apfel value) {
                return ByteString.copyFromUtf8("Apfel");
            }

            @Override
            public Apfel decode(ByteString bytes) {
                return new Apfel();
            }

        });
        RegisterKey<Birne> regB = Key.register("staticReadWrite_reg2", new ValueCoder<Birne>() {
            @Override
            public ByteString encode(Birne value) {
                return ByteString.copyFromUtf8("Birne");
            }

            @Override
            public Birne decode(ByteString bytes) {
                return new Birne();
            }

        });

        List<Obst> obstListe = bucket.readAll(antidoteClient.noTransaction(), Arrays.asList(regA, regB));
        assertEquals(Arrays.asList(new Apfel(), new Birne()), obstListe);


        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }
}
