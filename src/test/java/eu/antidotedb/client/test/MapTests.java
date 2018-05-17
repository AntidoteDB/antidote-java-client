package eu.antidotedb.client.test;

import eu.antidotedb.client.AntidoteStaticTransaction;
import eu.antidotedb.client.MapKey;
import eu.antidotedb.client.NoTransaction;
import eu.antidotedb.client.ResponseDecoder;
import org.junit.Test;

import java.util.*;

import static eu.antidotedb.client.Key.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the mutable CRDT types
 */
public class MapTests extends AbstractAntidoteTest {


    @Test
    public void testRef() {
        MapKey testmap = map_rr("testmap2");

        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        bucket.update(tx,
                testmap.update(
                        counter("a").increment(5),
                        register("b").assign("Hello")
                ));
        tx.commitTransaction();

        MapKey.MapReadResult res = bucket.read(antidoteClient.noTransaction(), testmap);
        assertEquals(5, (long) res.get(counter("a")));
        assertEquals("Hello", res.get(register("b")));

    }

    @Test
    public void testRefRemoveKey() {
        MapKey testmap = map_rr("testmap2");

        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        bucket.update(tx, testmap.update(
                counter("a").increment(5),
                multiValueRegister("b").assign("Hello")
        ));
        tx.commitTransaction();

        bucket.update(antidoteClient.noTransaction(), testmap.removeKeys(multiValueRegister("b")));

        MapKey.MapReadResult res = bucket.read(antidoteClient.noTransaction(), testmap);
        assertEquals(hashset(counter("a")), res.keySet());
        assertEquals(5, (long) res.get(counter("a")));
        assertEquals(null, res.get(register("b")));

    }

    @Test
    public void nonexistingKey() {
        List<String> res1 = bucket.read(antidoteClient.noTransaction(), set("doesnotexist"));
        assertEquals(Collections.emptyList(), res1);

        List<String> res2 = bucket.read(antidoteClient.noTransaction(), map_g("doesnotexist2")).get(set("S"));
        assertEquals(Collections.emptyList(), res2);

    }

//    @Test
//    public void nestedBatchRead() {
//        MapKey testmap = Key.map_aw("nestedBatchRead");
//        CounterKey a = counter("a");
//        CounterKey b = counter("b");
//
//        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
//        bucket.update(tx, testmap.update(
//                a.increment(3),
//                b.increment(4)
//        ));
//        tx.commitTransaction();
//
//        BatchRead batchRead = antidoteClient.newBatchRead();
//        BatchReadResult<Integer> aRes = bucket.read(batchRead, a);
//        BatchReadResult<Integer> bRes = bucket.read(batchRead, b);
//        batchRead.commit(antidoteClient.noTransaction());
//
//        assertEquals(3, (long) aRes.get());
//        assertEquals(4, (long) bRes.get());
//
//    }


//    @Test
//    public void testMutable() {
//        int readCount = messageCounter.getStaticReadsCounter();
//        int updateCount = messageCounter.getStaticUpdatesCounter();
//        {
//            MapKey testmapKey = Key.map_aw("testmap2");
//            CrdtMap<String, CrdtSet<String>> testmap = testmapRef.toMutable(CrdtSet.creator(ValueCoder.utf8String));
//
//            CrdtSet<String> a = testmap.get("a");
//            a.add("1");
//            a.add("2");
//            CrdtSet<String> b = testmap.get("b");
//            b.add("3");
//            testmap.push(antidoteClient.noTransaction());
//        }
//
//        {
//            CrdtMap<String, CrdtSet<String>> testmap = Key.map_aw("testmap2", ValueCoder.utf8String).toMutable(CrdtSet.creator(ValueCoder.utf8String));
//            testmap.pull(antidoteClient.noTransaction());
//
//
//            assertEquals(hashset("1", "2"), testmap.get("a").getValues());
//            assertEquals(hashset("3"), testmap.get("b").getValues());
//        }
//        // We expect that there was one static read transaction and one static write:
//        assertEquals(readCount + 1, messageCounter.getStaticReadsCounter());
//        assertEquals(updateCount + 1, messageCounter.getStaticUpdatesCounter());
//
//    }

    @Test
    public void testMapResult() {
        MapKey map = map_rr("blubmap");
        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        bucket.update(tx, map.update(
                counter("x").increment(1),
                counter("y").increment(2),
                counter("z").increment(3)
        ));
        tx.commitTransaction();

        MapKey.MapReadResult readResult = bucket.read(antidoteClient.noTransaction(), map);

        assertEquals(hashset(counter("x"), counter("y"), counter("z")), readResult.keySet());

        Map<String, Integer> result = readResult.asJavaMap(ResponseDecoder.counter());

        Map<String, Integer> expected = new HashMap<>();
        expected.put("x", 1);
        expected.put("y", 2);
        expected.put("z", 3);

        assertEquals(expected, result);
    }


    @Test
    public void testMapResult2() {
        MapKey map = map_rr("blubmap");
        NoTransaction tx = antidoteClient.noTransaction();
        bucket.update(tx, map.operation()
                .update(fatCounter("x").increment(1))
                .update(fatCounter("xx").increment(1))
                .update(fatCounter("y").increment(2))
                .update(fatCounter("yy").increment(2)));

        bucket.update(tx, map.operation()
                .removeKey(fatCounter("xx"))
                .update(fatCounter("z").increment(3))
                .removeKey(fatCounter("yy")));


        MapKey.MapReadResult readResult = bucket.read(tx, map);

        assertEquals(hashset(fatCounter("x"), fatCounter("y"), fatCounter("z")), readResult.keySet());

        Map<String, Integer> result = readResult.asJavaMap(ResponseDecoder.counter());

        Map<String, Integer> expected = new HashMap<>();
        expected.put("x", 1);
        expected.put("y", 2);
        expected.put("z", 3);

        assertEquals(expected, result);
    }


    @Test
    public void resetTest() {
        MapKey map = map_rr("aha");
        AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
        bucket.update(tx, map.update(
                set("x").addAll("1", "2", "3"),
                set("y").addAll("4", "5")
        ));
        tx.commitTransaction();

        Map<String, List<String>> res1 = bucket.read(antidoteClient.noTransaction(), map).asJavaMap(ResponseDecoder.set());
        Map<String, List<String>> expected = new HashMap<>();
        expected.put("x", Arrays.asList("1", "2", "3"));
        expected.put("y", Arrays.asList("4", "5"));
        assertEquals(expected, res1);

        bucket.update(antidoteClient.noTransaction(), map.reset());
        MapKey.MapReadResult res2 = bucket.read(antidoteClient.noTransaction(), map);
        assertTrue(res2.isEmpty());


    }

    private <T> Set<T> hashset(T... ts) {
        return new LinkedHashSet<>(Arrays.asList(ts));
    }
}
