package eu.antidotedb.client.test;

import eu.antidotedb.client.Key;
import eu.antidotedb.client.NoTransaction;
import eu.antidotedb.client.RegisterKey;
import eu.antidotedb.client.ValueCoder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class NoTxTest extends AbstractAntidoteTest {

    @Test
    public void withoutTransactions() {
        RegisterKey<String> reg = Key.register("withoutTransactions_reg1", ValueCoder.utf8String);
        NoTransaction tx = antidoteClient.noTransaction();
        bucket.update(tx, reg.assign("Abc"));
        assertEquals("Abc", bucket.read(tx, reg));
        bucket.update(tx, reg.assign("xyz"));
        assertEquals("xyz", bucket.read(tx, reg));
    }

    @Test
    public void defaultValue() {
        RegisterKey<String> reg = Key.register("empty", ValueCoder.utf8String);
        assertEquals("", bucket.read(antidoteClient.noTransaction(), reg));
    }
}
