package eu.antidotedb.client.test;

import eu.antidotedb.client.InteractiveTransaction;
import eu.antidotedb.client.RegisterRef;
import eu.antidotedb.client.ValueCoder;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class InteractiveTxTest extends AbstractAntidoteTest {

    @Test
    public void testEmptyTx() {
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
        }
    }

    @Test
    public void testInteractiveTx() {
        RegisterRef<String> reg = bucket.register("testInteractiveTx_reg1", ValueCoder.utf8String);
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            reg.set(tx, "Abc");
            tx.commitTransaction();
        }
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            assertEquals("Abc", reg.read(tx));
            reg.set(tx, "xyz");
            assertEquals("xyz", reg.read(tx));
            tx.commitTransaction();
        }
    }

    @Test
    public void testAbort() {
        RegisterRef<String> reg = bucket.register("testAbort", ValueCoder.utf8String);
        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            reg.set(tx, "Abc");
            tx.abortTransaction();
        }
        String x = reg.read(antidoteClient.noTransaction());
        assertEquals("", x);
    }


    @Test
    public void testMany() {
        IntStream.range(0, 100).parallel().forEach(i -> {
            RegisterRef<String> reg = bucket.register("testInteractiveTx_reg" + i, ValueCoder.utf8String);
            try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
                reg.set(tx, "" + i);
                tx.commitTransaction();
            }
        });
        RegisterRef<String> reg = bucket.register("testInteractiveTx_reg99", ValueCoder.utf8String);

        try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
            assertEquals("99", reg.read(tx));
            tx.commitTransaction();
        }
    }
}
