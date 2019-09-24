package eu.antidotedb.client.messages;


import eu.antidotedb.antidotepb.AntidotePB;

/**
 * Responses sent by Antidote on the Protocol Buffer interface.
 * <p>
 * New instance can be created using the methods {@link #of(AntidotePB.ApbErrorResp)}, {@link #of(AntidotePB.ApbCommitResp)},
 * {@link #of(AntidotePB.ApbOperationResp)}, {@link #of(AntidotePB.ApbReadObjectsResp)}, {@link #of(AntidotePB.ApbStartTransactionResp)}
 * and {@link #of(AntidotePB.ApbStaticReadObjectsResp)}.
 */
public abstract class AntidoteResponse extends AntidoteMessage {
    /**
     * A transformer for all possible cases of Antidote responses.
     * <p>
     * Uses double-dispatch to choose the correct implementation.
     *
     * @param <V> the return type of the handle methods
     * @see AntidoteResponse#accept(Handler)
     */
    public interface Handler<V> {
        default V handle(AntidotePB.ApbErrorResp op) {
            int code = op.getErrcode();
            String msg = op.getErrmsg().toStringUtf8();
            throw new ExtractionError(this.getClass() + " - Unexpected error message with errorcode " + code + "\n" + msg);
        }

        default V handle(AntidotePB.ApbOperationResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbStartTransactionResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbReadObjectsResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbCommitResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbStaticReadObjectsResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

        default V handle(AntidotePB.ApbGetConnectionDescriptorResp op) {
            throw new ExtractionError(this.getClass() + " - Unexpected message: " + op);
        }

    }

    /**
     * An exception thrown by response extractors when response type does not match.
     *
     * @see MsgCommitResp.Extractor
     * @see MsgReadObjectsResp.Extractor
     * @see MsgErrorResp.Extractor
     * @see MsgOperationResp.Extractor
     * @see MsgStartTransactionResp.Extractor
     * @see MsgStaticReadObjectsResp.Extractor
     */
    public static class ExtractionError extends RuntimeException {
        public ExtractionError(String message) {
            super(message);
        }
    }

    public static AntidoteResponse of(AntidotePB.ApbErrorResp op) {
        return new MsgErrorResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbOperationResp op) {
        return new MsgOperationResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbStartTransactionResp op) {
        return new MsgStartTransactionResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbReadObjectsResp op) {
        return new MsgReadObjectsResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbCommitResp op) {
        return new MsgCommitResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbStaticReadObjectsResp op) {
        return new MsgStaticReadObjectsResp(op);
    }

    public static AntidoteResponse of(AntidotePB.ApbGetConnectionDescriptorResp op) {
        return new MsgGetConnectionDescriptorResponse(op);
    }

    /**
     * Handle the response using the given response transformer.
     *
     * @param handler the implementation of the handling methods
     * @param <V>     the result type of the transformer methods
     * @return the result of the matching transformer method
     */
    public abstract <V> V accept(Handler<V> handler);

    public static class MsgErrorResp extends AntidoteResponse {
        private final AntidotePB.ApbErrorResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbErrorResp> {

            @Override
            public AntidotePB.ApbErrorResp handle(AntidotePB.ApbErrorResp op) {
                return op;
            }

        }

        private MsgErrorResp(AntidotePB.ApbErrorResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgErrorResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgOperationResp extends AntidoteResponse {
        private final AntidotePB.ApbOperationResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbOperationResp> {


            @Override
            public AntidotePB.ApbOperationResp handle(AntidotePB.ApbOperationResp op) {
                return op;
            }

        }

        private MsgOperationResp(AntidotePB.ApbOperationResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgOperationResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgStartTransactionResp extends AntidoteResponse {
        private final AntidotePB.ApbStartTransactionResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbStartTransactionResp> {


            @Override
            public AntidotePB.ApbStartTransactionResp handle(AntidotePB.ApbStartTransactionResp op) {
                return op;
            }

        }

        private MsgStartTransactionResp(AntidotePB.ApbStartTransactionResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgStartTransactionResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgReadObjectsResp extends AntidoteResponse {
        private final AntidotePB.ApbReadObjectsResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbReadObjectsResp> {


            @Override
            public AntidotePB.ApbReadObjectsResp handle(AntidotePB.ApbReadObjectsResp op) {
                return op;
            }

        }

        private MsgReadObjectsResp(AntidotePB.ApbReadObjectsResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgReadObjectsResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgCommitResp extends AntidoteResponse {
        private final AntidotePB.ApbCommitResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbCommitResp> {


            @Override
            public AntidotePB.ApbCommitResp handle(AntidotePB.ApbCommitResp op) {
                return op;
            }

        }

        private MsgCommitResp(AntidotePB.ApbCommitResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgCommitResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgStaticReadObjectsResp extends AntidoteResponse {
        private final AntidotePB.ApbStaticReadObjectsResp op;

        /**
         * A response transformer used to extract the encapsulated Protocol Buffer message
         */
        public static class Extractor implements Handler<AntidotePB.ApbStaticReadObjectsResp> {

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbErrorResp op) {
                throw new ExtractionError("MsgStaticReadObjectsResp: Unexpected message: " + op);
            }

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbOperationResp op) {
                throw new ExtractionError("MsgStaticReadObjectsResp: Unexpected message: " + op);
            }

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbStartTransactionResp op) {
                throw new ExtractionError("MsgStaticReadObjectsResp: Unexpected message: " + op);
            }

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbReadObjectsResp op) {
                throw new ExtractionError("MsgStaticReadObjectsResp: Unexpected message: " + op);
            }

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbCommitResp op) {
                throw new ExtractionError("MsgStaticReadObjectsResp: Unexpected message: " + op);
            }

            @Override
            public AntidotePB.ApbStaticReadObjectsResp handle(AntidotePB.ApbStaticReadObjectsResp op) {
                return op;
            }
        }

        private MsgStaticReadObjectsResp(AntidotePB.ApbStaticReadObjectsResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }

        @Override
        public String toString() {
            return "MsgStaticReadObjectsResp{" +
                    "op=" + op +
                    '}';
        }
    }

    public static class MsgGetConnectionDescriptorResponse extends AntidoteResponse {
        private AntidotePB.ApbGetConnectionDescriptorResp op;

        public static class Extractor implements Handler<AntidotePB.ApbGetConnectionDescriptorResp> {
            @Override
            public AntidotePB.ApbGetConnectionDescriptorResp handle(AntidotePB.ApbGetConnectionDescriptorResp op) {
                return op;
            }
        }

        private MsgGetConnectionDescriptorResponse(AntidotePB.ApbGetConnectionDescriptorResp op) {
            this.op = op;
        }

        @Override
        public <V> V accept(Handler<V> handler) {
            return handler.handle(op);
        }
    }
}
