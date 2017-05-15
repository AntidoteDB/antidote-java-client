package eu.antidotedb.client.transformer;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteResponse;

public class TransformerWithDownstream implements Transformer {
    private final Transformer downstream;

    public TransformerWithDownstream(Transformer downstream) {
        this.downstream = downstream;
    }

    /**
     * Get the request transformer below this layer. Can be used to forward requests to the next layer.
     *
     * @return The request transformer one layer down
     */
    public Transformer getDownstream() {
        return downstream;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbReadObjects op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbUpdateObjects op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStartTransaction op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbAbortTransaction op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbCommitTransaction op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticReadObjects op) {
        return downstream.handle(op);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticUpdateObjects op) {
        return downstream.handle(op);
    }
}
