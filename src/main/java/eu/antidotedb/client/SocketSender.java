package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.Transformer;

import java.io.IOException;
import java.net.Socket;


class SocketSender extends Transformer {


    private class AntidoteSocketException extends AntidoteException {
        public AntidoteSocketException(IOException cause) {
            super(cause);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbReadObjects op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbUpdateObjects op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStartTransaction op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbAbortTransaction op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            // no response expected
            return null;
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbCommitTransaction op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticReadObjects op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(Connection connection, AntidotePB.ApbStaticUpdateObjects op) {
        try {
            ApbCoder.encodeRequest(op, connection.getSocket().getOutputStream());
            return ApbCoder.decodeResponse(connection.getSocket().getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }
}