package utils;

/**
 * Created by moyong on 2017/10/31.
 */

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.rocksdb.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RocksJavaTest {
    private static org.apache.log4j.Logger log = Logger.getLogger(RocksJavaTest.class);

    private static final String dbPath = "datafam";

    static {
        RocksDB.loadLibrary();
    }

    RocksDB rocksDB;

    public RocksJavaTest() throws RocksDBException {

    }

    /**
     * 项目路径
     *
     * @param url
     * @return
     */
    public  String basePath() {
        URL url = this.getClass().getResource("/");

        String path = url.getPath();

        log.debug(path);

        String basepath = path.substring(0,path.indexOf("target"));

        log.debug(basepath);
        return basepath;
    }


//    public  Map<Object, ColumnFamilyHandle> columnFamilies=new HashMap<Object, ColumnFamilyHandle>();
    public BiMap<Object,ColumnFamilyHandle> columnFamilies = HashBiMap.create();

    public  RocksDB  getdb(List<ColumnFamilyHandle> columnFamilyHandles ) throws RocksDBException {
        //构造 cfd
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);
        //构造 cf list
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, basePath()+dbPath);

        log.debug("cfs size:"+cfs.size());

        if (cfs.size() > 0) {
            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
                log.debug("cfs name:"+new String(cf));
            }
        } else {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        //构造 cfh
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        rocksDB = RocksDB.open(dbOptions, basePath()+dbPath, columnFamilyDescriptors, columnFamilyHandles);

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

        cfs = RocksDB.listColumnFamilies(options,basePath()+ dbPath);

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


    @Test
    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public void testDefaultColumnFamily() throws RocksDBException {

        Options options = new Options();
        options.setCreateIfMissing(true);

        rocksDB = RocksDB.open(options, basePath()+dbPath);
        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksDB.put(key, value);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, basePath()+dbPath);
        log.debug("列出所有的 cf");

        for (byte[] cf : cfs) {
            log.debug(new String(cf));
        }

        log.debug("one key");
        byte[] getValue = rocksDB.get(key);
        log.debug(new String(getValue));

        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        log.debug("组合查询");
        Map<byte[], byte[]> valueMap = rocksDB.multiGet(keys);
        for (Map.Entry<byte[], byte[]> entry : valueMap.entrySet()) {
            log.debug(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        log.debug("遍历");

        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            log.debug("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

        log.debug("删除 one key 后遍历");

        rocksDB.delete(key);
        log.debug("after remove key:" + new String(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            log.debug("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

    }

    @Test
    public void testCertainColumnFamily() throws RocksDBException {
        String table = "CertainColumnFamilyTest";
        String key = "certainKey";
        String value = "certainValue";

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Options options = new Options();
        options.setCreateIfMissing(true);

        log.debug(""+basePath()+dbPath);
        //构造 cf list
        List<byte[]> cfs = RocksDB.listColumnFamilies(options, basePath()+dbPath);


        if (cfs.size() > 0) {
            log.debug("cfs size  : " + cfs.size());

            for (byte[] cf : cfs) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
                log.debug("=================columnFamilyHandles size:" + Arrays.equals(cf, "CertainColumnFamilyTest".getBytes()));
                log.debug("=================columnFamilyHandles size:" + new String(cf));

            }
        } else {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        }

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);

        rocksDB = RocksDB.open(dbOptions, basePath()+dbPath, columnFamilyDescriptors, columnFamilyHandles);
        log.debug("=================columnFamilyHandles size:" + columnFamilyHandles.size());

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
        log.debug("get Value : " + new String(getValue));

        rocksDB.put(columnFamilyHandle, "SecondKey".getBytes(), "SecondValue".getBytes());

//        List<byte[]> cfs1 = RocksDB.listColumnFamilies(options, dbPath);
//        for(byte[] cf : cfs1) {
//            log.debug(new String(cf));
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
            log.debug(new String(entry.getKey()) + "--" + new String(entry.getValue()));
        }


        rocksDB.delete(columnFamilyHandle, key.getBytes());
        log.debug("================remove after ...=1");

        RocksIterator iter = rocksDB.newIterator(columnFamilyHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            log.debug(new String(iter.key()) + ":" + new String(iter.value()));
        }


        log.debug("=================2 columnFamilyHandles.get(0)" + columnFamilyHandles.get(0));
        log.debug("=================2 columnFamilyHandles.get(1)" + columnFamilyHandles.get(1));

        final WriteBatch wb = new WriteBatch();
        wb.put(columnFamilyHandles.get(0), "ThreeKey".getBytes(), "ThreeValue".getBytes());
        wb.put(columnFamilyHandle, "4Key".getBytes(), "4Value".getBytes());
        rocksDB.write(new WriteOptions(), wb);

        RocksIterator iter3 = rocksDB.newIterator(columnFamilyHandles.get(0));
        for (iter3.seekToFirst(); iter3.isValid(); iter3.next()) {
            log.debug(new String(iter3.key()) + ":" + new String(iter3.value()));

//            rocksDB.del(iter3.key());

        }
//        ReadOptions ro = new ReadOptions();
//        ro.set
//        rocksDB.multiGet();
//        rocksDB.write();

        log.debug("=================3");

        List<RocksIterator> rocksIterators = rocksDB.newIterators(handleList);
        for (RocksIterator ri : rocksIterators
                ) {
            log.debug("************");

            for (ri.seekToFirst(); ri.isValid(); ri.next()) {
                log.debug(new String(ri.key()) + ":" + new String(ri.value()));
            }
        }

    }

    @Test
    public  void main() throws RocksDBException {
        //
        // 通过两次获取数据，一次是 mulget key;一次是 mulget ttl ;一次是 mulget size ;
        //
//        RocksJavaTest test = new RocksJavaTest();
//      test.testDefaultColumnFamily();
//        test.testCertainColumnFamily();

        List<ColumnFamilyHandle> cfhs = new ArrayList<>();

        RocksDB db = getdb(cfhs);

        log.debug("....cfhs:"+cfhs.size());


        

        //批量写
        //批量读
//        Map<Object, ColumnFamilyHandle> columnFamilies = columnFamilies;

        db.put("1".getBytes(),"1".getBytes());

        log.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        saveListData(db,"1".getBytes(),"1v".getBytes(),"ttl".getBytes(),"size".getBytes(),"pseq".getBytes(),"nseq".getBytes(),"cseq".getBytes());
        log.debug("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        getListData(db,"1".getBytes());
        log.debug("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

//        log.debug(columnFamilies);
//        log.debug(columnFamilies.get("ttl"));
//        log.debug("....cfhs:"+new String(db.get("1".getBytes())));
//
//        log.debug(db.get(columnFamilies.get("ttl"),"1".getBytes()));
//        log.debug(db.get(columnFamilies.get("size"),"1".getBytes()));
//        log.debug(db.get(columnFamilies.get("pseq"),"1".getBytes()));
//        log.debug(db.get(columnFamilies.get("nseq"),"1".getBytes()));
//        log.debug(db.get(columnFamilies.get("cseq"),"1".getBytes()));

        log.debug("<<<cleanBy<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //批量删除
        db.put("abc#1".getBytes(),"value".getBytes());
        db.put("abc#2".getBytes(),"value".getBytes());
        db.put("abc#3".getBytes(),"value".getBytes());
        db.put("abc#4".getBytes(),"value".getBytes());
        db.put("abc#5".getBytes(),"value".getBytes());
        db.put("abc#6".getBytes(),"value".getBytes());
        db.put("abd#1".getBytes(),"value".getBytes());
        db.put("abd#2".getBytes(),"value".getBytes());
        db.put("abe#3".getBytes(),"value".getBytes());
        db.put("abf#4".getBytes(),"value".getBytes());
        db.put("abg#5".getBytes(),"value".getBytes());
        db.put("abh#6".getBytes(),"value".getBytes());
        db.put("ach#6".getBytes(),"value".getBytes());

        //清除某个类型的数据
//        db.cleanBy("abc#".getBytes(),"abc#z".getBytes());
        db.deleteRange("abd#".getBytes(),"abd#".getBytes());

        RocksIterator iter = db.newIterator();
        for (iter.seek("ab".getBytes()); iter.isValid(); iter.next()) {
//            iter.status();
            log.debug(new String(iter.key()) + ":" + new String(iter.value()));
        }

//        db.cleanBy("abc3".getBytes(),"abf4".getBytes());


        db.compactRange();



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

        log.debug(db.get(columnFamilies.get("ttl"),"1ttl".getBytes()));
        log.debug(db.get(columnFamilies.get("size"),"1size".getBytes()));
        log.debug(db.get(columnFamilies.get("pseq"),"1pseq".getBytes()));
        log.debug(db.get(columnFamilies.get("nseq"),"1nseq".getBytes()));
        log.debug(db.get(columnFamilies.get("cseq"),"1cseq".getBytes()));

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
        log.debug(new String(array));


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

        log.debug( "======================="+ db.getLatestSequenceNumber());

        Map<byte[], byte[]> map = db.multiGet(handleList, keys);
        log.debug(map.keySet().size());

//        map.put(key,  db.get(key));

        //fixme 同一个 key 不同  cf 的数据不能通过 multiget 获取，业务上就不能利用 cf 保存相机的数据，除非 key 不一样
        Map<byte[], byte[]> multiGet = db.multiGet(handleList, keys);
        log.debug(multiGet.keySet().size());

        for (Map.Entry<byte[], byte[]> entry : multiGet.entrySet()) {
            log.debug(new String(entry.getKey()) + "--" + new String(entry.getValue()));
        }


        return multiGet;

    }

}
