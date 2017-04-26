package test.java.Tests;

import main.java.AntidoteClient.AntidoteClient;
import main.java.AntidoteClient.Connection;
import main.java.AntidoteClient.ConnectionPool;
import main.java.AntidoteClient.PoolManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * Created by kandarp on 26.04.17.
 */
public class AntidoteMoreTest {

    public static void main(String[] args) {
        try {
            PoolManager p = new PoolManager(20, 5);
            System.out.println(p);
            Socket s = p.getConnection().getSocket();
            System.out.println(s);
            DataOutputStream dataOutputStream = new DataOutputStream(s.getOutputStream());
            dataOutputStream.writeInt(1);
            dataOutputStream.writeByte(1);
            DataInputStream din = new DataInputStream(s.getInputStream());
            System.out.println(din.readUTF());

        } catch (Exception e) {
            System.out.println(e);
        }


    }

}

