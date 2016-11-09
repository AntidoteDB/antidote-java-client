import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;

import java.io.*;
import java.net.Socket;

public class AntidoteClient {
    private Socket socket;
    private String host;
    private int port;

    public AntidoteClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void updateCounter(String name, String bucket) {

        ApbStaticUpdateObjects.Builder counterUpdateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote

        ApbBoundObject.Builder counterObject = ApbBoundObject.newBuilder(); // The object in the message
        counterObject.setKey(ByteString.copyFromUtf8(name));
        counterObject.setType(CRDT_type.COUNTER);
        counterObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(1); // Set increment

        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(counterObject);
        updateInstruction.setOperation(updateOperation);

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        counterUpdateMessage.setTransaction(writeTransaction);
        counterUpdateMessage.addUpdates(updateInstruction);

        ApbStaticUpdateObjects counterUpdateMessageObject = counterUpdateMessage.build();

        int messageLength = counterUpdateMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 122; // todo: change this to enum

        // Message written and read as <length:32> <msg_code:8> <pbmsg>

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            counterUpdateMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(messageData);
            System.out.println(commitResponse);
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void readCounter(String name, String bucket) {

        ApbBoundObject.Builder counterObject = ApbBoundObject.newBuilder(); // The object in the message
        counterObject.setKey(ByteString.copyFromUtf8(name));
        counterObject.setType(CRDT_type.COUNTER);
        counterObject.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(counterObject);

        ApbStaticReadObjects readMessageObject = readMessage.build();

        int messageLength = readMessageObject.toByteArray().length + 1; // Protobuf length + message code
        int messageCode = 123; // todo: change this to enum

        try {
            socket = new Socket(host, port);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.writeByte(messageCode);
            readMessageObject.writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);


            ApbStaticReadObjectsResp readResponse = ApbStaticReadObjectsResp.parseFrom(messageData);

            System.out.println(readResponse);
            socket.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}