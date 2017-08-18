package eu.antidotedb.client.transformer;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteResponse;

import java.util.logging.Logger;

/**
 * A Transformer, which logs all messages
 */
public class LogTransformer extends TransformerWithDownstream {
    private Logger logger = Logger.getLogger("Antidote Messages");
    public static final TransformerFactory factory = (downstream, c) -> new LogTransformer(downstream);

    public LogTransformer(Transformer downstream) {
        super(downstream);
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbReadObjects op) {
        logger.info("ApbReadObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbUpdateObjects op) {
        logger.info("ApbUpdateObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStartTransaction op) {
        logger.info("ApbStartTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbAbortTransaction op) {
        logger.info("ApbAbortTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbCommitTransaction op) {
        logger.info("ApbCommitTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticReadObjects op) {
        logger.info("ApbStaticReadObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticUpdateObjects op) {
        logger.info("ApbStaticUpdateObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(op);
        logger.info("response -> " + response);
        return response;
    }
}
