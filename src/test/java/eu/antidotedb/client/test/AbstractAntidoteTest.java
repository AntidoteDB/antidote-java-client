package eu.antidotedb.client.test;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.PoolManager;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 *
 */
public class AbstractAntidoteTest {
    PoolManager antidotePoolManager;
    AntidoteClient antidoteClient;
    Bucket bucket;

    public AbstractAntidoteTest() {
        antidotePoolManager = new PoolManager(20, 5);
        antidoteClient = new AntidoteClient(antidotePoolManager);
        String bucketKey = nextSessionId();
        bucket = antidoteClient.bucket(bucketKey);
    }

    public String nextSessionId() {
        SecureRandom random = new SecureRandom();
        return new BigInteger(130, random).toString(32);
    }
}
