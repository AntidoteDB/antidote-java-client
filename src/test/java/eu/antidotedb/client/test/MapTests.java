package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            MapRef<String> testmapRef = bucket.map_aw("testmap2");
            CrdtMap<String, CrdtSet<String>> testmap = testmapRef.toMutable(CrdtSet.creator(ValueCoder.utf8String));

            CrdtSet<String> a = testmap.get("a");
            a.add("1");
            a.add("2");
            CrdtSet<String> b = testmap.get("b");
            b.add("3");
            testmap.push(antidoteClient.noTransaction());
        }

        {
            CrdtMap<String, CrdtSet<String>> testmap = bucket.map_aw("testmap2", ValueCoder.utf8String).toMutable(CrdtSet.creator(ValueCoder.utf8String));
            testmap.pull(antidoteClient.noTransaction());


            assertEquals(set("1", "2"), testmap.get("a").getValues());
            assertEquals(set("3"), testmap.get("b").getValues());
        }
        // We expect that there was one static read transaction and one static write:
        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());

    }

    @Test
    public void testMapResult() {
        MapRef<String> map = bucket.map_rr("blubmap");
        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        map.integer("x").set(tx, 1);
        map.integer("y").set(tx, 2);
        map.integer("z").set(tx, 3);
        tx.commitTransaction();

        MapRef.MapReadResult<String> readResult = map.read(antidoteClient.noTransaction());

        assertEquals(set("x", "y", "z"), readResult.keySet());

        Map<String, Long> result = readResult.asJavaMap(ResponseDecoder.integer());

        Map<String, Long> expected = new HashMap<>();
        expected.put("x", 1L);
        expected.put("y", 2L);
        expected.put("z", 3L);

        assertEquals(expected, result);
    }

    @Test
    public void resetTest() {
        MapRef<String> map = bucket.map_rr("aha");
        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        map.set("x").addAll(tx, Arrays.asList("1", "2", "3"));
        map.set("y").addAll(tx, Arrays.asList("4", "5"));
        tx.commitTransaction();

        Map<String, List<String>> res1 = map.read(antidoteClient.noTransaction()).asJavaMap(ResponseDecoder.set());
        Map<String, List<String>> expected = new HashMap<>();
        expected.put("x", Arrays.asList("1", "2", "3"));
        expected.put("y", Arrays.asList("4", "5"));
        assertEquals(expected, res1);

        map.reset(antidoteClient.noTransaction());
        Map<String, List<String>> res2 = map.read(antidoteClient.noTransaction()).asJavaMap(ResponseDecoder.set());
        assertTrue(res2.isEmpty());


    }

    private <T> Set<T> set(T... ts) {
        return new LinkedHashSet<>(Arrays.asList(ts));
    }
}
