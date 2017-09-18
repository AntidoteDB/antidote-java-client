package eu.antidotedb.client.test;

import eu.antidotedb.client.AntidoteClient;
import eu.antidotedb.client.AntidoteConfigManager;
import eu.antidotedb.client.Bucket;
import eu.antidotedb.client.transformer.CountingTransformer;
import eu.antidotedb.client.transformer.LogTransformer;
import eu.antidotedb.client.transformer.TransformerFactory;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * hint: before running the test start Antidote. For example with docker:
 * docker run --rm -p "8087:8087" antidotedb/antidote
 */
public class AbstractAntidoteTest {
    final static boolean debugLog = false;
    final CountingTransformer messageCounter;
    final AntidoteClient antidoteClient;
    final Bucket bucket;
    final String bucketKey;
    final SecureRandom random;

    public AbstractAntidoteTest() {
        random = new SecureRandom();
        
        List<TransformerFactory> transformers = new ArrayList<>();
        transformers.add(messageCounter = new CountingTransformer());
        if (debugLog) {
            transformers.add(LogTransformer.factory);
        }

        // load host config from xml file ...
        AntidoteConfigManager antidoteConfigManager = new AntidoteConfigManager();


        antidoteClient = new AntidoteClient(transformers, antidoteConfigManager.getConfigHosts());

        bucketKey = nextSessionId();
        bucket = Bucket.bucket(bucketKey);
    }

    public String nextSessionId() {
        return new BigInteger(130, random).toString(32);
    }
}
