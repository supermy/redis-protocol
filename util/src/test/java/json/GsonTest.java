package json;

import com.google.common.base.Strings;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.*;

public class GsonTest {
    private static Logger log = Logger.getLogger(GsonTest.class);

    /**
     * JsonElement的四个子类:JsonObject、JsonArray、JsonPrimitive、JsonNull
     *
     * JsonElement的子类，该类对Java的基本类型及其对应的对象类进行了封装
     * Java的基本数据类型（短整长，单精双精，字符<表示为单字符字符串>，布尔）
     * isBoolean()
     * isNumber()：包含短整长，单双精，字节
     * isPrimitiveOrString():包含字符和字符串
     *
     *
     * JsonNull:
     * 该类没什么可说的，为不可变类。当然在json中所有的JsonNullObject 调用equals方法判断的话都是相等的。
     *
     * JsonArray:
     * Json的数组包含的其实也是一个个Json串。所以不难猜出JsonArray中用一个集合类源码中用List<JsonElement>来添加json数组中的每个元素。(详见源码，很简单)
     *
     * JsonObject:
     * json对象类，包含了键值对，键是字符串类型，它的值是一个JsonElement。用 LinkedTreeMap<String, JsonElement> members来保存。
     *
     *
     */
    @Test
    public void testgson(){
        Gson gson = new Gson();
        Gson gson1 = new GsonBuilder().serializeNulls().create();

        Gson gson2 = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation() //不对没有用@Expose注解的属性进行操作
                .enableComplexMapKeySerialization() //当Map的key为复杂对象时,需要开启该方法
                .serializeNulls() //当字段值为空或null时，依然对该字段进行转换
                .setDateFormat("yyyy-MM-dd HH:mm:ss:SSS") //时间转化为特定格式
                .setPrettyPrinting() //对结果进行格式化，增加换行
                .disableHtmlEscaping() //防止特殊字符出现乱码
//                .registerTypeAdapter(User.class,new UserAdapter()) //为某特定对象设置固定的序列或反序列方式，自定义Adapter需实现JsonSerializer或者JsonDeserializer接口
                .create();


        Gson gson3 = new GsonBuilder().enableComplexMapKeySerialization().create(); //开启复杂处理Map方法
        Map<String,List<String>> map = new HashMap<String,List<String>>();

        String[] abc={"a","b","c"};
        List<String> strings = Arrays.asList(abc);
        map.put("abc",strings);

        String jsonStr = gson3.toJson(map);  //toJson
        System.out.println(jsonStr);

        Map<String,List<String>> resultMap = gson3.fromJson(jsonStr,new TypeToken<Map<String,List<String>>>() {}.getType()); //fromJson
        log.debug(resultMap);

        String ts="{" +
                "  \"title\": \"Java Puzzlers: Traps, Pitfalls, and Corner Cases\"," +
                "  \"isbn-10\": \"032133678X\"," +
                "  \"isbn-13\": \"978-0321336781\"," +
                "  \"authors\": [" +
                "    \"Joshua Bloch\"," +
                "    \"Neal Gafter\"" +
                "  ]" +
                "}";

        String ts2="{\"company\":{\"aliwwStatus\":0,\"id\":0,\"name\":\"红茄子印像\",\"sellerId\":\"1000000301\",\"trustScore\":0},\"count\":5,\"pagecount\":0,\"pageindex\":0,\"resultList\":[{\"atoms\":[{\"bankImage\":true,\"height\":300,\"imageURL\":\"http://web.tcloudapp.cn/temp/1000000001/18C3C7410D9B1E5B964E0BAA6268053A727510F2CAC5E70E72C3C7DE48AE1.jpg_640x400.jpg\",\"style\":0,\"title\":\"\",\"uIActionParams\":{},\"versionCode\":0,\"width\":640},{\"bankImage\":true,\"height\":300,\"imageURL\":\"http://web.tcloudapp.cn/temp/1000000001/1B0F08D14F1F793842AC87B17003C52AE408D8FCFD26628EF83E65C3CFFD2.jpg_640x400.jpg\",\"style\":0,\"title\":\"\",\"uIActionParams\":{},\"versionCode\":0,\"width\":640},{\"bankImage\":true,\"height\":300,\"imageURL\":\"http://web.tcloudapp.cn/temp/1000000001/5857874C9779EF6EFCAC9FD55702FA4D0EE5522F035F2E14A9972CC955123.jpg_640x400.jpg\",\"style\":0,\"title\":\"\",\"uIActionParams\":{},\"versionCode\":0,\"width\":640}],\"height\":300,\"type\":\"slides\",\"versionCode\":0,\"width\":640},{\"atoms\":[{\"action\":\"go_to_search\",\"bankImage\":true,\"colorBackground\":\"#333333\",\"height\":100,\"style\":1,\"title\":\"人气宝贝\",\"uIActionParams\":{\"offerRequest\":{\"beginPage\":0,\"companyId\":1000000301,\"deliveryFree\":false,\"descendOrder\":true,\"discount\":false,\"pageSize\":0,\"pop\":false,\"priceEnabled\":false,\"priceEnd\":3.4028235E38,\"priceStart\":0,\"sortType\":\"renqi\"}},\"versionCode\":0,\"width\":640}],\"height\":100,\"type\":\"channel_item\",\"versionCode\":0,\"width\":640},{\"atoms\":[{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img03.taobaocdn.com/bao/uploaded/i3/T1OdxDXXtvXXcWWv_a_090851.jpg_250x250.jpg\",\"offerId\":102077,\"originalPrice\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时包邮 8*8照片书|相册影集|婴儿|宝宝|儿童/diy定制|制作 简单\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img05.taobaocdn.com/bao/uploaded/i5/T1w9NDXipxXXay20Q4_052428.jpg_250x250.jpg\",\"offerId\":102076,\"originalPrice\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时包邮 8*8照片书|相册影集|婴儿|宝宝|儿童/diy定制|制作 D调\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img06.taobaocdn.com/bao/uploaded/i6/T1hIRDXalGXXa_aivX_085522.jpg_250x250.jpg\",\"offerId\":102075,\"originalPrice\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":119.30,\"cent\":11930,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时包邮 8*8照片书|相册影集|婴儿|宝宝|儿童/diy定制|制作 青春\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img01.taobaocdn.com/bao/uploaded/i1/T1GyuyXjxDXXcE22E8_100415.jpg_250x250.jpg\",\"offerId\":102074,\"originalPrice\":{\"amount\":158.00,\"cent\":15800,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":158.00,\"cent\":15800,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"马伊琍同款项链 蝴蝶 正品925纯银 假一罰十 DIY字母名字项链定做\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img02.taobaocdn.com/bao/uploaded/i2/T1Uw4IXmRyXXayTOM6_062232.jpg_250x250.jpg\",\"offerId\":102073,\"originalPrice\":{\"amount\":158.00,\"cent\":15800,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":158.00,\"cent\":15800,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"马伊琍同款项链 爱心天使 正品925纯银 假一罰十 DIY名字项链定做\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img08.taobaocdn.com/bao/uploaded/i8/T1vElPXcFEXXb6e.34_054443.jpg_250x250.jpg\",\"offerId\":102072,\"originalPrice\":{\"amount\":238.00,\"cent\":23800,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":238.00,\"cent\":23800,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"包邮！银条男士项链 正品925纯银刻字 实心项链 DIY刻字项链定做\",\"versionCode\":0,\"volume\":0,\"width\":213}],\"height\":578,\"type\":\"offer_item\",\"versionCode\":0,\"width\":640},{\"atoms\":[{\"action\":\"go_to_search\",\"bankImage\":true,\"colorBackground\":\"#333333\",\"height\":100,\"style\":1,\"title\":\"最新上市\",\"uIActionParams\":{\"offerRequest\":{\"beginPage\":0,\"companyId\":1000000301,\"deliveryFree\":false,\"descendOrder\":true,\"discount\":false,\"pageSize\":0,\"pop\":false,\"priceEnabled\":false,\"priceEnd\":3.4028235E38,\"priceStart\":0,\"sortType\":\"time\"}},\"versionCode\":0,\"width\":640}],\"height\":100,\"type\":\"channel_item\",\"versionCode\":0,\"width\":640},{\"atoms\":[{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img01.taobaocdn.com/bao/uploaded/i1/T17OxOXi0yXXau1o75_060407.jpg_250x250.jpg\",\"offerId\":102041,\"originalPrice\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时抢购 跨年台历定制/照片定做/制作/挂历/日历 8寸25页\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img08.taobaocdn.com/bao/uploaded/i8/T12yhOXh0EXXXfz.I5_060343.jpg_250x250.jpg\",\"offerId\":102042,\"originalPrice\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时抢购 跨年台历定制/照片定做/制作/挂历/日历 8寸25页 宝宝\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img05.taobaocdn.com/bao/uploaded/i5/T1fpx1XfdoXXcDATUZ_033624.jpg_250x250.jpg\",\"offerId\":102043,\"originalPrice\":{\"amount\":44.80,\"cent\":4480,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":44.80,\"cent\":4480,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时秒杀 8x6画册|相册/照片书/影集|宝宝儿童/diy定制|制作 生活\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img06.taobaocdn.com/bao/uploaded/i6/T1jyVOXntsXXaQrU35_060401.jpg_250x250.jpg\",\"offerId\":102044,\"originalPrice\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":46.80,\"cent\":4680,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时抢购 跨年台历定制/照片定做/制作/挂历/日历 8寸25页 看海\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img01.taobaocdn.com/bao/uploaded/i1/T1yrX1XjJbXXcDvJfa_090502.jpg_250x250.jpg\",\"offerId\":102045,\"originalPrice\":{\"amount\":44.80,\"cent\":4480,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":44.80,\"cent\":4480,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"限时秒杀 8x6画册|相册/照片书/影集|宝宝儿童/diy定制|制作 曾经\",\"versionCode\":0,\"volume\":0,\"width\":213},{\"bankImage\":true,\"height\":289,\"imageURL\":\"http://img05.taobaocdn.com/bao/uploaded/i5/T1rUROXcNAXXX_AA.6_062529.jpg_250x250.jpg\",\"offerId\":102046,\"originalPrice\":{\"amount\":39.59,\"cent\":3959,\"currencyCode\":\"CNY\"},\"price\":{\"amount\":39.59,\"cent\":3959,\"currencyCode\":\"CNY\"},\"style\":0,\"title\":\"卖疯了！国际一级白瓷！个性水杯定制 印图照片 情侣马克杯 春日\",\"versionCode\":0,\"volume\":0,\"width\":213}],\"height\":578,\"type\":\"offer_item\",\"versionCode\":0,\"width\":640}],\"totalCount\":0}\n";

        JsonObject jsonObject = gson3.fromJson(ts, JsonObject.class);
        JsonObject jsonObject2 = gson3.fromJson(ts2, JsonObject.class);

//        Iterator<Map.Entry<String, JsonElement>> iterator = jsonObject2.entrySet().iterator();

        log.debug("------------------------------------------");

        StringBuilder result=new StringBuilder();
        bl(jsonObject2,"0",0,0,result);

        String[] split = result.toString().split("````");
        List<String> strings1 = Arrays.asList(split);
        Collections.sort(strings1);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        strings1.forEach(a->System.out.println(a));


        log.debug("------------------------------------------");
        log.debug(jsonObject);
        log.debug("------------------------------------------");

        log.debug(jsonObject2);
        log.debug("------------------------------------------");


        JsonObject jo=new JsonObject();
        jo.addProperty("a",1);
        jo.addProperty("b","2");
        log.debug(gson3.toJson(jo));

    }

    /**
     * 索引：宽度遍历优先
     *
     * 记录关系使用_from _to
     * json 是单向关系；graph 是双向关系；
     *
     * 层级+pid 为key
     * 字符串记录树形结构
     * 11
     * 11111
     * 22
     * 22222
     * 33
     * 3333333
     *
     * @param je
     * @param level 层级
     * @param index 序号
     * @return
     */
    public int bl(JsonElement je,String  level,int parent,int index,StringBuilder result) {

//        StringBuilder sb=new StringBuilder();

        if (je.isJsonObject()){
//            level++;
//            parent;

            level=level+parent;

            System.out.println("");

            //记录关系
            Iterator<Map.Entry<String, JsonElement>> iterator = je.getAsJsonObject().entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, JsonElement> next = iterator.next();


                index++;
                parent++;


                System.out.print(level+"-"+index+"~"+next.getKey()+":");

                result.append("````").append(level).append("-").append(index).append("~").append(next.getKey()).append(":");


                index=bl(next.getValue(),level,parent,index,result);
            }
        }

        if (je.isJsonArray()){
            parent = index;

            System.out.println("");
            //记录关系；
            JsonArray asJsonArray = je.getAsJsonArray();
            for (JsonElement json:asJsonArray){
                index=bl(json,level,parent,index,result);
                System.out.println("");
            }
        }

        if (je.isJsonNull()){
            System.out.println("================");
        }

        if (je.isJsonPrimitive()){
            result.append(je);
            System.out.println(je);
        }

        return index;
    }

}
