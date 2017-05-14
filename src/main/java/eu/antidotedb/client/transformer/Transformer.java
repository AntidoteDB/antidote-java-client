package eu.antidotedb.client.transformer;


import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.Connection;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;

/**
 * Transforms an Antidote request. Is again an Antidote request handler.
 */
public abstract class Transformer {
    private Transformer downstream;

    /**
     * Get the request handler below this layer. Can be used to forward requests to the next layer.
     * @return The request handler one layer down
     */
    public Transformer getDownstream() {
        return downstream;
    }

    /**
     * Connect the transformer to the next layer below.
     * @param downstream The underlying request handler
     */
    public void connect(Transformer downstream) {
        this.downstream = downstream;
    }


    public AntidoteResponse handle(Connection connection, AntidotePB.ApbReadObjects op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbUpdateObjects op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStartTransaction op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbAbortTransaction op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbCommitTransaction op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticReadObjects op) {
        return downstream.handle(connection, op);
    }

    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticUpdateObjects op) {
        return downstream.handle(connection, op);
    }

    public AntidoteRequest.Handler<AntidoteResponse> toHandler(Connection c) {
        return new AntidoteRequest.Handler<AntidoteResponse>() {
            @Override
            public AntidoteResponse handle(AntidotePB.ApbReadObjects op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbUpdateObjects op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbStartTransaction op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbAbortTransaction op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbCommitTransaction op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbStaticReadObjects op) {
                return Transformer.this.handle(c, op);
            }

            @Override
            public AntidoteResponse handle(AntidotePB.ApbStaticUpdateObjects op) {
                return Transformer.this.handle(c, op);
            }
        };
    }
}
