package eu.antidotedb.client.messages;


import eu.antidotedb.antidotepb.AntidotePB;

/**
 * Requests sent by Antidote on the Protocol Buffer interface.
 * <p>
 * New instance can be created using the methods {@link #of(AntidotePB.ApbReadObjects)}, {@link #of(AntidotePB.ApbUpdateObjects)},
 * {@link #of(AntidotePB.ApbAbortTransaction)}, {@link #of(AntidotePB.ApbStartTransaction)}, {@link #of(AntidotePB.ApbCommitTransaction)},
 * {@link #of(AntidotePB.ApbStaticReadObjects)} and {@link #of(AntidotePB.ApbStaticUpdateObjects)}.
 */
public abstract class AntidoteRequest<Response> extends AntidoteMessage {
    /**
     * A handler for all possible cases of Antidote requests.
     * <p>
     * Uses double-dispatch to choose the correct implementation.
     *
     * @param <V> the return type of the handle methods
     * @see AntidoteRequest#accept(Handler)
     */
    public interface Handler<V> {
        default V handle(AntidotePB.ApbReadObjects op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbUpdateObjects op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbStartTransaction op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbAbortTransaction op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbCommitTransaction op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbStaticReadObjects op) {
            throw new ExtractionError("Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbStaticUpdateObjects op) {
            throw new ExtractionError("Unexpected message: " + op);
        }
    }

    /**
     * An exception thrown by response extractors when response type does not match.
     *
     * @see MsgReadObjects.Extractor
     * @see MsgUpdateObjects.Extractor
     * @see MsgStartTransaction.Extractor
     * @see MsgAbortTransaction.Extractor
     * @see MsgCommitTransaction.Extractor
     * @see MsgStaticReadObjects.Extractor
     * @see MsgStaticUpdateObjects.Extractor
     */
    public static class ExtractionError extends RuntimeException {
        public ExtractionError(String message) {
            super(message);
        }
    }

    public static MsgReadObjects of(AntidotePB.ApbReadObjects op) {
        return new MsgReadObjects(op);
    }

    public static MsgUpdateObjects of(AntidotePB.ApbUpdateObjects op) {
        return new MsgUpdateObjects(op);
    }

    public static MsgStartTransaction of(AntidotePB.ApbStartTransaction op) {
        return new MsgStartTransaction(op);
    }

    public static MsgAbortTransaction of(AntidotePB.ApbAbortTransaction op) {
        return new MsgAbortTransaction(op);
    }

    public static MsgCommitTransaction of(AntidotePB.ApbCommitTransaction op) {
        return new MsgCommitTransaction(op);
    }

    public static MsgStaticReadObjects of(AntidotePB.ApbStaticReadObjects op) {
        return new MsgStaticReadObjects(op);
    }

    public static MsgStaticUpdateObjects of(AntidotePB.ApbStaticUpdateObjects op) {
        return new MsgStaticUpdateObjects(op);
    }

    /**
     * Handle the request using the given request handler.
     *
     * @param handler the implementation of the handling methods
     * @param <V>     the result type of the handler methods
     * @return the result of the matching handler method
     */
    public abstract <V> V accept(Handler<V> handler);

    /**
     * @return An extractor for the expected response or null if no response is expected
     */
    public abstract AntidoteResponse.Handler<Response> readResponseExtractor();


    public static class MsgReadObjects extends AntidoteRequest<AntidotePB.ApbReadObjectsResp> {
        private AntidotePB.ApbReadObjects op;

        public static class Extractor implements Handler<AntidotePB.ApbReadObjects> {

            @Override
            public AntidotePB.ApbReadObjects handle(AntidotePB.ApbReadObjects op) {
                return op;
            }

        }

        private MsgReadObjects(AntidotePB.ApbReadObjects op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbReadObjectsResp> readResponseExtractor() {
            return new AntidoteResponse.MsgReadObjectsResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgReadObjects{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgUpdateObjects extends AntidoteRequest<AntidotePB.ApbOperationResp> {
        private AntidotePB.ApbUpdateObjects op;

        public static class Extractor implements Handler<AntidotePB.ApbUpdateObjects> {


            @Override
            public AntidotePB.ApbUpdateObjects handle(AntidotePB.ApbUpdateObjects op) {
                return op;
            }

        }

        private MsgUpdateObjects(AntidotePB.ApbUpdateObjects op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbOperationResp> readResponseExtractor() {
            return new AntidoteResponse.MsgOperationResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgUpdateObjects{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgStartTransaction extends AntidoteRequest<AntidotePB.ApbStartTransactionResp> {
        private AntidotePB.ApbStartTransaction op;

        public static class Extractor implements Handler<AntidotePB.ApbStartTransaction> {


            @Override
            public AntidotePB.ApbStartTransaction handle(AntidotePB.ApbStartTransaction op) {
                return op;
            }

        }

        private MsgStartTransaction(AntidotePB.ApbStartTransaction op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbStartTransactionResp> readResponseExtractor() {
            return new AntidoteResponse.MsgStartTransactionResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgStartTransaction{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgAbortTransaction extends AntidoteRequest<Void> {
        private AntidotePB.ApbAbortTransaction op;

        public static class Extractor implements Handler<AntidotePB.ApbAbortTransaction> {


            @Override
            public AntidotePB.ApbAbortTransaction handle(AntidotePB.ApbAbortTransaction op) {
                return op;
            }

        }

        private MsgAbortTransaction(AntidotePB.ApbAbortTransaction op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<Void> readResponseExtractor() {
            return null;
        }

        @Override
        public String toString() {
            return "MsgAbortTransaction{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgCommitTransaction extends AntidoteRequest<AntidotePB.ApbCommitResp> {
        private AntidotePB.ApbCommitTransaction op;

        public static class Extractor implements Handler<AntidotePB.ApbCommitTransaction> {


            @Override
            public AntidotePB.ApbCommitTransaction handle(AntidotePB.ApbCommitTransaction op) {
                return op;
            }

        }

        private MsgCommitTransaction(AntidotePB.ApbCommitTransaction op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbCommitResp> readResponseExtractor() {
            return new AntidoteResponse.MsgCommitResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgCommitTransaction{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgStaticReadObjects extends AntidoteRequest<AntidotePB.ApbStaticReadObjectsResp> {
        private AntidotePB.ApbStaticReadObjects op;

        public static class Extractor implements Handler<AntidotePB.ApbStaticReadObjects> {


            @Override
            public AntidotePB.ApbStaticReadObjects handle(AntidotePB.ApbStaticReadObjects op) {
                return op;
            }

        }

        private MsgStaticReadObjects(AntidotePB.ApbStaticReadObjects op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbStaticReadObjectsResp> readResponseExtractor() {
            return new AntidoteResponse.MsgStaticReadObjectsResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgStaticReadObjects{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgStaticUpdateObjects extends AntidoteRequest<AntidotePB.ApbCommitResp> {
        private AntidotePB.ApbStaticUpdateObjects op;

        public static class Extractor implements Handler<AntidotePB.ApbStaticUpdateObjects> {


            @Override
            public AntidotePB.ApbStaticUpdateObjects handle(AntidotePB.ApbStaticUpdateObjects op) {
                return op;
            }
        }

        private MsgStaticUpdateObjects(AntidotePB.ApbStaticUpdateObjects op) {
            this.op = op;
        }

        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public AntidoteResponse.Handler<AntidotePB.ApbCommitResp> readResponseExtractor() {
            return new AntidoteResponse.MsgCommitResp.Extractor();
        }

        @Override
        public String toString() {
            return "MsgStaticUpdateObjects{" +
                    "op=" + op +
                    '}';
        }
    }
}
