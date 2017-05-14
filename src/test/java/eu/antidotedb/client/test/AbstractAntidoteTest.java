package eu.antidotedb.client.test;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.PoolManager;
import eu.antidotedb.client.transformer.CountingTransformer;
import eu.antidotedb.client.transformer.LogTransformer;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 *
 */
public class AbstractAntidoteTest {
    final static boolean debugLog = false;
    final CountingTransformer messageCounter;
    final AntidoteClient antidoteClient;
    final Bucket<String> bucket;
    final String bucketKey;

    public AbstractAntidoteTest() {
        PoolManager antidotePoolManager = new PoolManager(20, 5);
        antidoteClient = new AntidoteClient(antidotePoolManager);
        // uncomment line to add logging:
        if (debugLog) {
            antidoteClient.addTransformer(new LogTransformer());
        }
        antidoteClient.addTransformer(messageCounter = new CountingTransformer());
        bucketKey = nextSessionId();
        bucket = Bucket.create(bucketKey);
    }

    public String nextSessionId() {
        SecureRandom random = new SecureRandom();
        return new BigInteger(130, random).toString(32);
    }
}
