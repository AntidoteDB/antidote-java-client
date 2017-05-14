package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

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
}
