package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

/**
 * A default implementation for inner update operations.
 * Simply stores the Apb update operation
 */
class UpdateOpDefaultImpl extends UpdateOp {
    private final AntidotePB.ApbUpdateOperation.Builder updateOperation;

    UpdateOpDefaultImpl(Key<?> key, AntidotePB.ApbUpdateOperation.Builder updateOperation) {
        super(key);
        this.updateOperation = updateOperation;
    }

    @Override
    AntidotePB.ApbUpdateOperation.Builder getApbUpdate() {
        return updateOperation;
    }
}
