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

            @Override
            public Apfel cast(Object value) {
                return (Apfel) value;
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

            @Override
            public Birne cast(Object value) {
                return (Birne) value;
            }
        });

        List<Obst> obstListe = bucket.readAll(antidoteClient.noTransaction(), Arrays.asList(regA, regB));
        assertEquals(Arrays.asList(new Apfel(), new Birne()), obstListe);


        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }
}
