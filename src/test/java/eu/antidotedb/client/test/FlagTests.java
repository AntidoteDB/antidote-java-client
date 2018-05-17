package eu.antidotedb.client.test;

import eu.antidotedb.client.AntidoteStaticTransaction;
import eu.antidotedb.client.FlagKey;
import eu.antidotedb.client.Key;
import org.junit.Test;

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
}
