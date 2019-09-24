package eu.antidotedb.client;

import com.google.protobuf.GeneratedMessageV3;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static eu.antidotedb.client.MessageCodes.*;

/**
 * Created by mweber on 12.04.17.
 */
public class ApbCoder {
    public static AntidoteRequest<?> decodeRequest(InputStream stream) throws IOException {
        byte[] sizeRaw = new byte[4];
        readFully(stream, sizeRaw);
        ByteBuffer buffer = ByteBuffer.wrap(sizeRaw);
        buffer.order(ByteOrder.BIG_ENDIAN);
        int size = buffer.getInt();
        int msgCode = stream.read();
        if (msgCode == -1) {
            throw new IOException("End of input while message code expected");
        }
        byte[] data = new byte[size - 1];
        readFully(stream, data);
//        DataInputStream dataInputStream = new DataInputStream(stream);
//        int size = dataInputStream.readInt();
//        int msgCode = dataInputStream.readByte();
//        byte[] data = new byte[size - 1];
//        dataInputStream.readFully(data, 0, size - 1);
        switch (msgCode) {
            case ApbReadObjects:
                AntidotePB.ApbReadObjects apbReadObjects = AntidotePB.ApbReadObjects.parseFrom(data);
                return AntidoteRequest.of(apbReadObjects);
            case ApbUpdateObjects:
                AntidotePB.ApbUpdateObjects apbUpdateObjects = AntidotePB.ApbUpdateObjects.parseFrom(data);
                return AntidoteRequest.of(apbUpdateObjects);
            case ApbStartTransaction:
                AntidotePB.ApbStartTransaction apbStartTransaction = AntidotePB.ApbStartTransaction.parseFrom(data);
                return AntidoteRequest.of(apbStartTransaction);
            case ApbAbortTransaction:
                AntidotePB.ApbAbortTransaction apbAbortTransaction = AntidotePB.ApbAbortTransaction.parseFrom(data);
                return AntidoteRequest.of(apbAbortTransaction);
            case ApbCommitTransaction:
                AntidotePB.ApbCommitTransaction apbCommitTransaction = AntidotePB.ApbCommitTransaction.parseFrom(data);
                return AntidoteRequest.of(apbCommitTransaction);
            case ApbStaticUpdateObjects:
                AntidotePB.ApbStaticUpdateObjects apbStaticUpdateObjects = AntidotePB.ApbStaticUpdateObjects.parseFrom(data);
                return AntidoteRequest.of(apbStaticUpdateObjects);
            case ApbStaticReadObjects:
                AntidotePB.ApbStaticReadObjects apbStaticReadObjects = AntidotePB.ApbStaticReadObjects.parseFrom(data);
                return AntidoteRequest.of(apbStaticReadObjects);
            case ApbCreateDC:
                AntidotePB.ApbCreateDC apbCreateDC = AntidotePB.ApbCreateDC.parseFrom(data);
                return AntidoteRequest.of(apbCreateDC);
            case ApbConnectToDCs:
                AntidotePB.ApbConnectToDCs apbConnectToDcs = AntidotePB.ApbConnectToDCs.parseFrom(data);
                return AntidoteRequest.of(apbConnectToDcs);
            case ApbGetConnectionDescriptor:
                AntidotePB.ApbGetConnectionDescriptor apbGetConnectionDescriptor = AntidotePB.ApbGetConnectionDescriptor.parseFrom(data);
                return AntidoteRequest.of(apbGetConnectionDescriptor);
            default:
                throw new RuntimeException("Unexpected request message code: " + msgCode);
        }
    }

    public static AntidoteResponse decodeResponse(InputStream stream) throws IOException {
        byte[] sizeRaw = new byte[4];
        readFully(stream, sizeRaw);
        ByteBuffer buffer = ByteBuffer.wrap(sizeRaw);
        buffer.order(ByteOrder.BIG_ENDIAN);
        int size = buffer.getInt();
        int msgCode = stream.read();
        if (msgCode == -1) {
            throw new IOException("End of input while message code expected");
        }
        byte[] data = new byte[size - 1];
        readFully(stream, data);
//        DataInputStream dataInputStream = new DataInputStream(stream);
//        int size = dataInputStream.readInt();
//        int msgCode = dataInputStream.readByte();
//        byte[] data = new byte[size - 1];
//        dataInputStream.readFully(data, 0, size - 1);

        switch (msgCode) {
            case 0:
                AntidotePB.ApbErrorResp apbErrorResp = AntidotePB.ApbErrorResp.parseFrom(data);
                return AntidoteResponse.of(apbErrorResp);
            case 111:
                AntidotePB.ApbOperationResp apbOperationResp = AntidotePB.ApbOperationResp.parseFrom(data);
                return AntidoteResponse.of(apbOperationResp);
            case 124:
                AntidotePB.ApbStartTransactionResp apbStartTransactionResp = AntidotePB.ApbStartTransactionResp.parseFrom(data);
                return AntidoteResponse.of(apbStartTransactionResp);
            case 126:
                AntidotePB.ApbReadObjectsResp apbReadObjectsResp = AntidotePB.ApbReadObjectsResp.parseFrom(data);
                return AntidoteResponse.of(apbReadObjectsResp);
            case 127:
                AntidotePB.ApbCommitResp apbCommitResp = AntidotePB.ApbCommitResp.parseFrom(data);
                return AntidoteResponse.of(apbCommitResp);
            case 128:
                AntidotePB.ApbStaticReadObjectsResp apbStaticReadObjectsResp = AntidotePB.ApbStaticReadObjectsResp.parseFrom(data);
                return AntidoteResponse.of(apbStaticReadObjectsResp);
            case ApbGetConnectionDescriptorResponse:
                AntidotePB.ApbGetConnectionDescriptorResp apbGetConnectionDescriptorResponse = AntidotePB.ApbGetConnectionDescriptorResp.parseFrom(data);
                return AntidoteResponse.of(apbGetConnectionDescriptorResponse);
            default:
                throw new RuntimeException("Unexpected message code: " + msgCode);
        }
    }

    private static void readFully(InputStream stream, byte[] data) throws IOException {
        int offset = 0;
        while (offset < data.length) {
            int bytesRead = stream.read(data, offset, data.length - offset);
            if (bytesRead == -1) {
                throw new IOException("End of input while data expected");
            }
            offset += bytesRead;
        }
    }

    public static void encodeRequest(AntidotePB.ApbReadObjects op, OutputStream stream) {
        encode(116, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbUpdateObjects op, OutputStream stream) {
        encode(118, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbStartTransaction op, OutputStream stream) {
        encode(119, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbAbortTransaction op, OutputStream stream) {
        encode(120, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbCommitTransaction op, OutputStream stream) {
        encode(121, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbStaticReadObjects op, OutputStream stream) {
        encode(123, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbStaticUpdateObjects op, OutputStream stream) {
        encode(122, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbCreateDC op, OutputStream stream) {
        encode(ApbCreateDC, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbConnectToDCs op, OutputStream stream) {
        encode(ApbConnectToDCs, op, stream);
    }

    public static void encodeRequest(AntidotePB.ApbGetConnectionDescriptor op, OutputStream stream) {
        encode(ApbGetConnectionDescriptor, op, stream);
    }


    public static void encodeResponse(AntidotePB.ApbOperationResp op, OutputStream stream) {
        encode(111, op, stream);
    }

    public static void encodeResponse(AntidotePB.ApbStartTransactionResp op, OutputStream stream) {
        encode(124, op, stream);
    }

    public static void encodeResponse(AntidotePB.ApbReadObjectsResp op, OutputStream stream) {
        encode(126, op, stream);
    }

    public static void encodeResponse(AntidotePB.ApbCommitResp op, OutputStream stream) {
        encode(127, op, stream);
    }

    public static void encodeResponse(AntidotePB.ApbStaticReadObjectsResp op, OutputStream stream) {
        encode(128, op, stream);
    }

    public static void encodeResponse(AntidotePB.ApbGetConnectionDescriptorResp op, OutputStream stream) {
        encode(ApbGetConnectionDescriptorResponse, op, stream);
    }

    private static void encode(int msgCode, GeneratedMessageV3 msg, OutputStream stream) {
        int serializedSize = msg.getSerializedSize();
        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(serializedSize + 1);
        buffer.put((byte) msgCode);
        OutputStream os = new BufferedOutputStream(stream);
        try {
            os.write(buffer.array());
            msg.writeTo(os);
            os.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void encodeRequest(AntidoteRequest<?> request, OutputStream stream) {
        request.accept(new AntidoteRequest.Handler<Void>() {
            @Override
            public Void handle(AntidotePB.ApbReadObjects op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbUpdateObjects op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbStartTransaction op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbAbortTransaction op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbCommitTransaction op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbStaticReadObjects op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbStaticUpdateObjects op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbCreateDC op) {
                encodeRequest(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbConnectToDCs op) {
                encodeRequest(op, stream);
                return null;
            }
        });
    }

    public static void encodeResponse(AntidoteResponse response, OutputStream stream) {
        response.accept(new AntidoteResponse.Handler<Void>() {
            @Override
            public Void handle(AntidotePB.ApbOperationResp op) {
                encodeResponse(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbStartTransactionResp op) {
                encodeResponse(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbReadObjectsResp op) {
                encodeResponse(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbCommitResp op) {
                encodeResponse(op, stream);
                return null;
            }

            @Override
            public Void handle(AntidotePB.ApbStaticReadObjectsResp op) {
                encodeResponse(op, stream);
                return null;
            }
        });
    }
}
