package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOp;

import java.util.List;

/**
 * A transaction, either static (batch of updates) or interactive (mixed reads and writes)
 */
public abstract class AntidoteTransaction implements UpdateContext {

    abstract void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction);

    abstract void performUpdates(List<ApbUpdateOp.Builder> updateInstructions);
}