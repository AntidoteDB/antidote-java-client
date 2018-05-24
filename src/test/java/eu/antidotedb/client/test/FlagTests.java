package eu.antidotedb.client.test;

import eu.antidotedb.client.*;
import org.junit.Test;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class FlagTests extends AbstractAntidoteTest {
    @Test
    public void flagewTest() {
        FlagKey flagKey = Key.flag_ew("flag1");

        AntidoteStaticTransaction txn = antidoteClient.createStaticTransaction();
        bucket.update(txn, flagKey.assign(true));
        txn.commitTransaction();

        Boolean flagVal = bucket.read(antidoteClient.noTransaction(), flagKey);
        assertEquals(true, flagVal);
    }

    @Test
    public void flagdwTest() {
        FlagKey flagKey = Key.flag_dw("flag2");

        AntidoteStaticTransaction txn = antidoteClient.createStaticTransaction();
        bucket.update(txn, flagKey.assign(true));
        txn.commitTransaction();

        Boolean flagVal = bucket.read(antidoteClient.noTransaction(), flagKey);
        assertEquals(true, flagVal);
    }

    @Test
    public void flagModifyInTx() {
        FlagKey flagKey = Key.flag_dw("flag3");

        InteractiveTransaction txn = antidoteClient.startTransaction();
        bucket.update(txn, flagKey.assign(false));
        Boolean valInsideTx1 = bucket.read(txn, flagKey);
        txn.commitTransaction();

        txn = antidoteClient.startTransaction();
        bucket.update(txn, flagKey.assign(true));
        Boolean valInsideTx2 = bucket.read(txn, flagKey);
        txn.commitTransaction();

        Boolean valOutsideTx = bucket.read(antidoteClient.noTransaction(), flagKey);

        assertEquals(false, valInsideTx1);
        assertEquals(true, valInsideTx2);
        assertEquals(true, valOutsideTx);
    }

    @Test
    public void flagInMap() {
        MapKey mapWithFlag = Key.map_g("map");

        InteractiveTransaction txn = antidoteClient.startTransaction();

        // read initial state
        MapKey.MapReadResult initialRead = bucket.read(txn, mapWithFlag);

        FlagKey flagKey = Key.flag_dw("innerFlag");
        bucket.update(txn, mapWithFlag.update(
                flagKey.assign(false)
        ));


        bucket.update(txn, mapWithFlag.update(flagKey.assign(true)));

        Boolean valInsideTx2 = bucket.read(txn, mapWithFlag).get(flagKey);

        txn.commitTransaction();
        Boolean valOutsideTx = bucket.read(antidoteClient.noTransaction(), mapWithFlag).get(flagKey);

        assertEquals(true, valInsideTx2);
        assertEquals(true, valOutsideTx);
    }

    @Test
    public void regInMap() {
        MapKey mapWithFlag = Key.map_g("map");

        InteractiveTransaction txn = antidoteClient.startTransaction();

        // read initial state
        MapKey.MapReadResult initialRead = bucket.read(txn, mapWithFlag);

        MVRegisterKey<String> regKey = Key.multiValueRegister("innerFlag");
        bucket.update(txn, mapWithFlag.update(
                regKey.assign("test1")
        ));


        bucket.update(txn, mapWithFlag.update(regKey.assign("test2")));

        List<String> valInsideTx2 = bucket.read(txn, mapWithFlag).get(regKey);

        txn.commitTransaction();
        List<String> valOutsideTx = bucket.read(antidoteClient.noTransaction(), mapWithFlag).get(regKey);

        assertEquals(singletonList("test2"), valInsideTx2);
        assertEquals(singletonList("test2"), valOutsideTx);
    }

    @Test
    public void readWriteWriteReg() {
        MVRegisterKey<String> regKey = Key.multiValueRegister("testReg");
        InteractiveTransaction txn = antidoteClient.startTransaction();

        // read initial state
        List<String> initialRead = bucket.read(txn, regKey);

        bucket.update(txn, regKey.assign("test1"));

        bucket.update(txn, regKey.assign("test2"));

        List<String> valInsideTx2 = bucket.read(txn, regKey);

        txn.commitTransaction();
        List<String> valOutsideTx = bucket.read(antidoteClient.noTransaction(), regKey);

        assertEquals(singletonList("test2"), valInsideTx2);
        assertEquals(singletonList("test2"), valOutsideTx);
    }
}
