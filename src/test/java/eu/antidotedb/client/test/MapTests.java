package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests the mutable CRDT types
 */
public class MapTests extends AbstractAntidoteTest {


    @Test
    public void testRef() {
        MapRef<String> testmap = bucket.map_aw("testmap2");

        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        testmap.counter("a").increment(tx, 5);
        testmap.register("b", ValueCoder.utf8String).set(tx, "Hello");
        tx.commitTransaction();

        MapRef.MapReadResult<String> res = testmap.read(antidoteClient.noTransaction());
        assertEquals(5, res.counter("a"));
        assertEquals("Hello", res.register("b", ValueCoder.utf8String));

    }

    @Test
    public void nestedBatchRead() {
        MapRef<String> testmap = bucket.map_aw("nestedBatchRead");
        CounterRef a = testmap.counter("a");
        CounterRef b = testmap.counter("b");

        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        a.increment(tx, 3);
        b.increment(tx, 4);
        tx.commitTransaction();

        BatchRead batchRead = antidoteClient.newBatchRead();
        BatchReadResult<Integer> aRes = a.read(batchRead);
        BatchReadResult<Integer> bRes = b.read(batchRead);

        assertEquals(3, (long) aRes.get());
        assertEquals(4, (long) bRes.get());

    }


    @Test
    public void testMutable() {
        int readCount = messageCounter.getStaticReadsCounter();
        int updateCount = messageCounter.getStaticUpdatesCounter();
        {
            CrdtMap<String, CrdtSet<String>> testmap = bucket.map_aw("testmap2", ValueCoder.utf8String).getMutable(CrdtSet.creator(ValueCoder.utf8String));

            CrdtSet<String> a = testmap.get("a");
            a.add("1");
            a.add("2");
            CrdtSet<String> b = testmap.get("b");
            b.add("3");
            testmap.push(antidoteClient.noTransaction());
        }

        {
            CrdtMap<String, CrdtSet<String>> testmap = bucket.map_aw("testmap", ValueCoder.utf8String).getMutable(CrdtSet.creator(ValueCoder.utf8String));
            testmap.pull(antidoteClient.noTransaction());



            assertEquals(set("1", "2"), testmap.get("a").getValues());
            assertEquals(set("3"), testmap.get("b").getValues());
        }
        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());

    }

    private <T> Set<T> set(T...ts) {
        return new LinkedHashSet<>(Arrays.asList(ts));
    }
}
