package guava;

/**
 * Created by moyong on 2017/10/31.
 */

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RocksJavaTest {

    private static final String dbPath = "datafam";

    static {
        RocksDB.loadLibrary();
    }

    RocksDB rocksDB;

    public RocksJavaTest() throws RocksDBException {

    }

//    public  Map<Object, ColumnFamilyHandle> columnFamilies=new HashMap<Object, ColumnFamilyHandle>();
    public BiMap<Object,ColumnFamilyHandle> columnFamilies = HashBiMap.create();

    public  RocksDB  getdb(List<ColumnFamilyHandle> columnFamilyHandles ) throws RocksDBException {
        //构造 cfd
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);
        //构造 cf list
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);

        System.out.println("cfs size:"+cfs.size());

        if (cfs.size() > 0) {
            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
                System.out.println("cfs name:"+new String(cf));
            }
        } else {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        //构造 cfh
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);

        byte[] ttl = "ttl".getBytes();
        byte[] size = "size".getBytes();
        byte[] pseq = "pseq".getBytes();
        byte[] nseq = "nseq".getBytes();
        byte[] cseq = "cseq".getBytes();

        createCF(columnFamilyDescriptors, ttl, columnFamilyHandles);
        createCF(columnFamilyDescriptors, size, columnFamilyHandles);
        createCF(columnFamilyDescriptors, pseq, columnFamilyHandles);
        createCF(columnFamilyDescriptors, nseq, columnFamilyHandles);
        createCF(columnFamilyDescriptors, cseq, columnFamilyHandles);

        cfs = RocksDB.listColumnFamilies(options, dbPath);

        for(int i=0;i<cfs.size();i++) {
            byte[] b = cfs.get(i);
            if(!Arrays.equals(b, RocksDB.DEFAULT_COLUMN_FAMILY)) {
//                Object value = cfSerializer.byteArrayToObject(b);
                columnFamilies.put(new String(b), columnFamilyHandles.get(i));
            }
        }


        return rocksDB;
    }

    public void put(RocksDB db,String cfName,byte[] key,byte[] value){
        try {
            db.put(columnFamilies.get(cfName),key,value);
        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
        }
    }

    public void put(RocksDB db,String cfName,byte[][] key,byte[][] value){
        try {
            for (int i = 0; i <key.length ; i++) {
                db.put(columnFamilies.get(cfName),key[i],value[i]);
            }
        } catch (RocksDBException e) {
//            throw new RedisException(e.getMessage());
        }
    }

    public byte[] get(RocksDB db, String cfName, byte[] key) throws RocksDBException {
        return db.get(columnFamilies.get(cfName),key);
    }



    /**
     * 创建并且返回列表 handles
     *
     * @param columnFamilyDescriptors
     * @param ttl
     * @param columnFamilyHandles
     * @return
     * @throws RocksDBException
     */
    public boolean createCF(List<ColumnFamilyDescriptor> columnFamilyDescriptors, byte[] table, List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
        boolean have = false;
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            if (Arrays.equals(columnFamilyDescriptors.get(i).columnFamilyName(),table)) {
                have = true;
            }
        }
        if (!have) {
            ColumnFamilyHandle cf = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(table, new ColumnFamilyOptions()));
            columnFamilyHandles.add(cf);
            return true;
        } else
            return false;
    }


    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public void testDefaultColumnFamily() throws RocksDBException {

        Options options = new Options();
        options.setCreateIfMissing(true);

        rocksDB = RocksDB.open(options, dbPath);
        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksDB.put(key, value);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);
        System.out.println("列出所有的 cf");

        for (byte[] cf : cfs) {
            System.out.println(new String(cf));
        }

        System.out.println("one key");
        byte[] getValue = rocksDB.get(key);
        System.out.println(new String(getValue));

        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        System.out.println("组合查询");
        Map<byte[], byte[]> valueMap = rocksDB.multiGet(keys);
        for (Map.Entry<byte[], byte[]> entry : valueMap.entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        System.out.println("遍历");

        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

        System.out.println("删除 one key 后遍历");

        rocksDB.delete(key);
        System.out.println("after remove key:" + new String(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

    }

    public void testCertainColumnFamily() throws RocksDBException {
        String table = "CertainColumnFamilyTest";
        String key = "certainKey";
        String value = "certainValue";

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);

        //构造 cf list
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, dbPath);


        if (cfs.size() > 0) {
            System.out.println("cfs size  : " + cfs.size());

            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
                System.out.println("=================columnFamilyHandles size:" + Arrays.equals(cf, "CertainColumnFamilyTest".getBytes()));
                System.out.println("=================columnFamilyHandles size:" + new String(cf));

            }
        } else {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);

        rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
        System.out.println("=================columnFamilyHandles size:" + columnFamilyHandles.size());

        byte[] key11 = "Hello".getBytes();
        byte[] value11 = "World".getBytes();
        rocksDB.put(key11, value11);

        //CertainColumnFamilyTest 删除
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            if (new String(columnFamilyDescriptors.get(i).columnFamilyName()).equals(table)) {
                rocksDB.dropColumnFamily(columnFamilyHandles.get(i));
            }
        }

        //重新重建 并且赋值
        ColumnFamilyHandle columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(table.getBytes(), new ColumnFamilyOptions()));


        rocksDB.put(columnFamilyHandle, key.getBytes(), value.getBytes());

        byte[] getValue = rocksDB.get(columnFamilyHandle, key.getBytes());
        System.out.println("get Value : " + new String(getValue));

        rocksDB.put(columnFamilyHandle, "SecondKey".getBytes(), "SecondValue".getBytes());

//        List<byte[]> cfs1 = RocksDB.listColumnFamilies(options, dbPath);
//        for(byte[] cf : cfs1) {
//            System.out.println(new String(cf));
//        }


        List<byte[]> keys = new ArrayList<byte[]>();
        keys.add(key.getBytes());
        keys.add("SecondKey".getBytes());
//        keys.add("ThreeKey".getBytes());

        //同一个 cfh 取值
        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        handleList.add(columnFamilyHandle);
        handleList.add(columnFamilyHandle);
//        handleList.add(columnFamilyHandles.get(0));

        Map<byte[], byte[]> multiGet = rocksDB.multiGet(handleList, keys);
        for (Map.Entry<byte[], byte[]> entry : multiGet.entrySet()) {
            System.out.println(new String(entry.getKey()) + "--" + new String(entry.getValue()));
        }


        rocksDB.delete(columnFamilyHandle, key.getBytes());
        System.out.println("================remove after ...=1");

        RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }


        System.out.println("=================2 columnFamilyHandles.get(0)" + columnFamilyHandles.get(0));
        System.out.println("=================2 columnFamilyHandles.get(1)" + columnFamilyHandles.get(1));

        final WriteBatch wb = new WriteBatch();
        wb.put(columnFamilyHandles.get(0), "ThreeKey".getBytes(), "ThreeValue".getBytes());
        wb.put(columnFamilyHandle, "4Key".getBytes(), "4Value".getBytes());
        rocksDB.write(new WriteOptions(), wb);

        RocksIterator iter3 = rocksDB.newIterator(columnFamilyHandles.get(0));
        for (iter3.seekToFirst(); iter3.isValid(); iter3.next()) {
            System.out.println(new String(iter3.key()) + ":" + new String(iter3.value()));

//            rocksDB.delete(iter3.key());

        }

        System.out.println("=================3");

        List<RocksIterator> rocksIterators = rocksDB.newIterators(handleList);
        for (RocksIterator ri : rocksIterators
                ) {
            System.out.println("************");

            for (ri.seekToFirst(); ri.isValid(); ri.next()) {
                System.out.println(new String(ri.key()) + ":" + new String(ri.value()));
            }
        }

    }

    public static void main(String[] args) throws RocksDBException {
        //
        // 通过两次获取数据，一次是 mulget key;一次是 mulget ttl ;一次是 mulget size ;
        //
        RocksJavaTest test = new RocksJavaTest();
//      test.testDefaultColumnFamily();
//        test.testCertainColumnFamily();

        List<ColumnFamilyHandle> cfhs = new ArrayList<>();

        RocksDB db = test.getdb(cfhs);

        System.out.println("....cfhs:"+cfhs.size());

        //批量写
        //批量读
        Map<Object, ColumnFamilyHandle> columnFamilies = test.columnFamilies;

        db.put("1".getBytes(),"1".getBytes());

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        test.saveListData(db,"1".getBytes(),"1v".getBytes(),"ttl".getBytes(),"size".getBytes(),"pseq".getBytes(),"nseq".getBytes(),"cseq".getBytes());
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        test.getListData(db,"1".getBytes());
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

//        System.out.println(columnFamilies);
//        System.out.println(columnFamilies.get("ttl"));
//        System.out.println("....cfhs:"+new String(db.get("1".getBytes())));
//
//        System.out.println(db.get(columnFamilies.get("ttl"),"1".getBytes()));
//        System.out.println(db.get(columnFamilies.get("size"),"1".getBytes()));
//        System.out.println(db.get(columnFamilies.get("pseq"),"1".getBytes()));
//        System.out.println(db.get(columnFamilies.get("nseq"),"1".getBytes()));
//        System.out.println(db.get(columnFamilies.get("cseq"),"1".getBytes()));

        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");



    }

    /**
     * 保存 list 数据
     * @param db
     * @param key
     * @param val
     * @param ttl
     * @param size
     * @param pseq
     * @param nseq
     * @param cseq
     * @throws RocksDBException
     */
    public  void saveListData(RocksDB db,byte[] key,byte[] val,byte[] ttl,byte[] size,byte[] pseq,byte[] nseq,byte[] cseq) throws RocksDBException {
        final WriteBatch wb = new WriteBatch();
        wb.put(key,val);
        wb.put(columnFamilies.get("ttl"),"1ttl".getBytes(),ttl);
        wb.put(columnFamilies.get("size"),"1size".getBytes(),size);
        wb.put(columnFamilies.get("pseq"),"1pseq".getBytes(),pseq);
        wb.put(columnFamilies.get("nseq"),"1nseq".getBytes(),nseq);
        wb.put(columnFamilies.get("cseq"),"1cseq".getBytes(),cseq);

        db.write(new WriteOptions(), wb);

        System.out.println(db.get(columnFamilies.get("ttl"),"1ttl".getBytes()));
        System.out.println(db.get(columnFamilies.get("size"),"1size".getBytes()));
        System.out.println(db.get(columnFamilies.get("pseq"),"1pseq".getBytes()));
        System.out.println(db.get(columnFamilies.get("nseq"),"1nseq".getBytes()));
        System.out.println(db.get(columnFamilies.get("cseq"),"1cseq".getBytes()));

    }

    public byte[] genFmKey(byte[] key, String fmname) {
//        ByteBuf fmBuf = Unpooled.directBuffer(16);
//        fmBuf.writeBytes(fmname.getBytes());
//
//
//        ByteBuf key0Buf = Unpooled.wrappedBuffer(key);

        ByteBuf keyBuf = Unpooled.wrappedBuffer(key,fmname.getBytes());
        keyBuf.resetReaderIndex();

        byte[] array = keyBuf.readBytes(keyBuf.readableBytes()).array();

//        return array;
        System.out.println(new String(array));


        return key;


    }

    public byte[] genFmKey(byte[] key, byte[] fmname) {
        ByteBuf ttlBuf = Unpooled.wrappedBuffer(key, fmname);

        return ttlBuf.readBytes(ttlBuf.readableBytes()).array();

    }

    public Map<byte[], byte[]> getListData(RocksDB db, byte[] key) throws RocksDBException {


        List<byte[]> keys = new ArrayList<byte[]>();

        keys.add("1ttl".getBytes());
        keys.add("1size".getBytes());
        keys.add("1pseq".getBytes());
        keys.add("1nseq".getBytes());
        keys.add("1cseq".getBytes());

        //同一个 cfh 取值
        List<ColumnFamilyHandle> handleList = new ArrayList<>();
        handleList.add(columnFamilies.get("ttl"));
        handleList.add(columnFamilies.get("size"));
        handleList.add(columnFamilies.get("pseq"));
        handleList.add(columnFamilies.get("nseq"));
        handleList.add(columnFamilies.get("cseq"));


//
//        db.get(columnFamilies.get("ttl"),key);
//        db.get(columnFamilies.get("size"),key);
//        db.get(columnFamilies.get("pseq"),key);
//        db.get(columnFamilies.get("nseq"),key);
//        db.get(columnFamilies.get("cseq"),key);

        System.out.println( "======================="+ db.getLatestSequenceNumber());

        Map<byte[], byte[]> map = db.multiGet(handleList, keys);
        System.out.println(map.keySet().size());

//        map.put(key,  db.get(key));

        //fixme 同一个 key 不同  cf 的数据不能通过 multiget 获取，业务上就不能利用 cf 保存相机的数据，除非 key 不一样
        Map<byte[], byte[]> multiGet = db.multiGet(handleList, keys);
        System.out.println(multiGet.keySet().size());

        for (Map.Entry<byte[], byte[]> entry : multiGet.entrySet()) {
            System.out.println(new String(entry.getKey()) + "--" + new String(entry.getValue()));
        }


        return multiGet;

    }

}
