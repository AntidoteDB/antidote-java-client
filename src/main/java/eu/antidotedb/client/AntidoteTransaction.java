package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOp;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * A transaction, either static (batch of updates) or interactive (mixed reads and writes)
 */
public abstract class AntidoteTransaction implements UpdateContext {

    abstract void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction);

    abstract void performUpdates(Collection<ApbUpdateOp.Builder> updateInstructions);


    /**
     * Called when a connection is acquired by the transaction
     */
    protected void onGetConnection(Connection connection) {
        // do nothing
    }

    /**
     * Called when a connection is released by the transaction
     */
    protected void onReleaseConnection(Connection connection) {
        // do nothing
    }
}