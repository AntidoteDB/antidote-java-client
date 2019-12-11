package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.Transformer;

import java.io.IOException;
import java.net.Socket;


class SocketSender implements Transformer {

    private Socket socket;

    public SocketSender(Socket s) {

        socket = s;
    }

    public static class AntidoteSocketException extends AntidoteException {
        public AntidoteSocketException(IOException cause) {
            super(cause);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbReadObjects op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbUpdateObjects op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStartTransaction op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbAbortTransaction op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbCommitTransaction op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticReadObjects op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbStaticUpdateObjects op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbCreateDC op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbConnectToDCs op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }

    @Override
    public AntidoteResponse handle(AntidotePB.ApbGetConnectionDescriptor op) {
        try {
            ApbCoder.encodeRequest(op, socket.getOutputStream());
            return ApbCoder.decodeResponse(socket.getInputStream());
        } catch (IOException e) {
            throw new AntidoteSocketException(e);
        }
    }
}