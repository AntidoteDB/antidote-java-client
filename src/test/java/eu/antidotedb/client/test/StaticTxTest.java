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

        RegisterRef<String> reg1 = bucket.register("staticReadWrite_reg1", ValueCoder.utf8String);
        RegisterRef<String> reg2 = bucket.register("staticReadWrite_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        reg1.set(stx, "a");
        reg2.set(stx, "b");
        stx.commitTransaction();

        BatchRead br = antidoteClient.newBatchRead();
        BatchReadResult<String> v1 = reg1.read(br);
        BatchReadResult<String> v2 = reg2.read(br);
        br.commit();

        assertEquals("a", v1.get());
        assertEquals("b", v2.get());

        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }


    @Test
    public void staticReadWrite2() {
        // same as above, but using implicit commit

        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();

        RegisterRef<String> reg1 = bucket.register("staticReadWrite_reg1", ValueCoder.utf8String);
        RegisterRef<String> reg2 = bucket.register("staticReadWrite_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        reg1.set(stx, "a");
        reg2.set(stx, "b");
        stx.commitTransaction();

        BatchRead br = antidoteClient.newBatchRead();
        BatchReadResult<String> v1 = reg1.read(br);
        BatchReadResult<String> v2 = reg2.read(br);

        assertEquals("a", v1.get());
        assertEquals("b", v2.get());

        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }

    @Test
    public void staticReadWrite3() {
        // same as above, but using implicit commit

        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();

        RegisterRef<String> reg1 = bucket.register("staticReadWrite_reg1", ValueCoder.utf8String);
        RegisterRef<String> reg2 = bucket.register("staticReadWrite_reg2", ValueCoder.utf8String);

        AntidoteStaticTransaction stx = antidoteClient.createStaticTransaction();
        reg1.set(stx, "a");
        reg2.set(stx, "b");
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

        RegisterRef<Apfel> regA = bucket.register("staticReadWrite_reg1", new ValueCoder<Apfel>() {
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
        RegisterRef<Birne> regB = bucket.register("staticReadWrite_reg2", new ValueCoder<Birne>() {
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

        List<Obst> obstListe = antidoteClient.readObjects(antidoteClient.noTransaction(), Arrays.asList(regA, regB));
        assertEquals(Arrays.asList(new Apfel(), new Birne()), obstListe);



        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
    }
}
