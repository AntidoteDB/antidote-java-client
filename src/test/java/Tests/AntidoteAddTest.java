package test.java.Tests;

import main.java.AntidoteClient.*;


import java.math.BigInteger;
import java.security.SecureRandom;

/**
 * Created by Kandarp on 16.04.17.
 */
public class AntidoteAddTest {

    public static void main(String[] args) {

        PoolManager antidotePoolManager;
        AntidoteClient antidoteClient;
        String bucket;
        AntidoteTransaction antidoteTransaction;
        antidotePoolManager = new PoolManager(20, 5);
//        antidotePoolManager.addHost(20, 5, new Host("localhost", 8087));
        antidoteClient = new AntidoteClient(antidotePoolManager);
        bucket = new BigInteger(130, new SecureRandom()).toString(32);

        antidoteTransaction = antidoteClient.createTransaction();
        CounterRef lowCounter = new CounterRef("testCounter5", bucket, antidoteClient);
        AntidoteOuterCounter counter = lowCounter.createAntidoteCounter(antidoteTransaction);
        int oldValue = counter.getValue();
//        counter.increment(antidoteTransaction);
//        counter.increment(antidoteTransaction);
//        antidoteTransaction.commitTransaction();
//        int newValue = counter.getValue();
//        System.out.println(antidotePoolManager);

    }
}
