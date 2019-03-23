package redis.server.netty.utis;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;

public class DbUtils {

    public static Roaring64NavigableMap importBitmap(byte[] r64nm) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(r64nm);
        DataInputStream dis = new DataInputStream(bais);
        Roaring64NavigableMap r11=new Roaring64NavigableMap();
        r11.deserialize(dis);
        bais.close();
        dis.close();
        return r11;
    }

    public static ByteArrayOutputStream exportBitmap(Roaring64NavigableMap r1) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        r1.serialize(dos);
        dos.flush();
        dos.close();
        return baos;
    }
}
