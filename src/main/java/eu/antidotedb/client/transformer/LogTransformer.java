package eu.antidotedb.client.transformer;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.Connection;
import eu.antidotedb.client.messages.AntidoteResponse;

import java.util.logging.Logger;

/**
 *A Transformer, which logs all messages
 */
public class LogTransformer extends Transformer {
    private Logger logger = Logger.getLogger("Antidote Messages");

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbReadObjects op) {
        logger.info("ApbReadObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbUpdateObjects op) {
        logger.info("ApbUpdateObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStartTransaction op) {
        logger.info("ApbStartTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbAbortTransaction op) {
        logger.info("ApbAbortTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbCommitTransaction op) {
        logger.info("ApbCommitTransaction <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticReadObjects op) {
        logger.info("ApbStaticReadObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticUpdateObjects op) {
        logger.info("ApbStaticUpdateObjects <<\n" + op + "\n>>");
        AntidoteResponse response = super.handle(connection, op);
        logger.info("response -> " + response);
        return response;
    }
}
