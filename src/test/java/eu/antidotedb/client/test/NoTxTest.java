package eu.antidotedb.client.test;

import eu.antidotedb.client.InteractiveTransaction;
import eu.antidotedb.client.NoTransaction;
import eu.antidotedb.client.RegisterRef;
import eu.antidotedb.client.ValueCoder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class NoTxTest extends AbstractAntidoteTest {

    @Test
    public void withoutTransactions() {
        RegisterRef<String> reg = bucket.register("withoutTransactions_reg1", ValueCoder.utf8String);
        NoTransaction tx = antidoteClient.noTransaction();
        reg.set(tx, "Abc");
        assertEquals("Abc", reg.read(tx));
        reg.set(tx, "xyz");
        assertEquals("xyz", reg.read(tx));
    }
}
