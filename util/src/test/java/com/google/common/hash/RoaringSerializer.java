package com.google.common.hash;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;

/**
 * kryo 结合 roaringbitmap 序列化
 * kryo 对于bloomfilter与Roaring都没有效果
 */
public class RoaringSerializer extends Serializer<Roaring64NavigableMap> {

    @Override
    public void write(Kryo kryo, Output output, Roaring64NavigableMap bitmap) {
        try {
            bitmap.serialize(new KryoDataOutput(output));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     *
     * roaringbitmap序列化
     *
     * @param kryo
     * @param input
     * @param type
     * @return
     */
    @Override
    public Roaring64NavigableMap read(Kryo kryo, Input input, Class<Roaring64NavigableMap> type) {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        try {
            bitmap.deserialize(new KryoDataInput(input));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        return bitmap;
    }

}