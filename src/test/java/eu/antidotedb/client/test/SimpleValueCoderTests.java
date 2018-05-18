package eu.antidotedb.client.test;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.ValueCoder;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SimpleValueCoderTests {
    @Test
    public void integerCoder() {
        Random random = new Random();
        for (int i = 0; i<1000; i++) {
            int orig = random.nextInt();
            ByteString encoded = ValueCoder.integerCoder.encode(orig);
            int decoded = ValueCoder.integerCoder.decode(encoded);
            assertEquals(orig, decoded);
        }
    }

    @Test
    public void longCoder() {
        Random random = new Random();
        for (int i = 0; i<1000; i++) {
            long orig = random.nextLong();
            ByteString encoded = ValueCoder.longCoder.encode(orig);
            long decoded = ValueCoder.longCoder.decode(encoded);
            assertEquals(orig, decoded);
        }
    }

    @Test
    public void floatCoder() {
        Random random = new Random();
        for (int i = 0; i<1000; i++) {
            float orig = random.nextFloat();
            ByteString encoded = ValueCoder.floatCoder.encode(orig);
            float decoded = ValueCoder.floatCoder.decode(encoded);
            assertEquals(orig, decoded, 0.0001);
        }
    }

    @Test
    public void doubleCoder() {
        Random random = new Random();
        for (int i = 0; i<1000; i++) {
            double orig = random.nextDouble();
            ByteString encoded = ValueCoder.doubleCoder.encode(orig);
            double decoded = ValueCoder.doubleCoder.decode(encoded);
            assertEquals(orig, decoded, 0.0001);
        }
    }
}
