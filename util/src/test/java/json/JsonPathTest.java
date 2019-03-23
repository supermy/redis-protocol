package json;

import com.google.common.cache.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.Cache;
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class JsonPathTest {
    private static Logger log = Logger.getLogger(JsonPathTest.class);

    /**
     *
     */
    @Test
    public void testJsonPath() {

        String json = "{\"objs\" : [{\"obj\" : 19740423},{\"obj2\" : 19740423}],\"id\":1}";
        DocumentContext ext = JsonPath.parse(json);
        log.debug(ext.jsonString());

        JsonPath p = JsonPath.compile("$.objs[0].obj");

//        JsonPath.read(json, "$.objs[0].obj");

        int a = ext.read(p);
        log.debug(a);

        ext.set(p, 141145561197333L);

        long aa = ext.read(p);
        log.debug(aa);


        String json1="{\"books\":[{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"fiction\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"},{\"category\":\"reference\"}]}";
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).build();

        //google 的cache 或者定义存储到rocksdb
        CacheProvider.setCache(new Cache() {

//        //Not thread safe simple cache
//        private Map<String, JsonPath> map = new HashMap<String, JsonPath>();
//
//        @Override
//        public JsonPath get(String key) {
//            return map.get(key);
//        }
//
//        @Override
//        public void put(String key, JsonPath jsonPath) {
//            map.put(key, jsonPath);
//        }


            com.google.common.cache.Cache<String, JsonPath> caches = CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .build();

            @Override
            public JsonPath get(String key) {
                log.debug("get===========key:" + key);

                return caches.getIfPresent(key);
            }

            @Override
            public void put(String key, JsonPath jsonPath) {
                log.debug("put===========key:" + key);

                caches.put(key, jsonPath);
            }


        });


        DocumentContext context = JsonPath.using(conf).parse(json1);
        log.debug(context.jsonString());
        context.delete("$.books[?(@.category == 'reference')]");
        List<String> categories = context.read("$..category", List.class);//fixme string array 多种类型的支撑
        log.debug(categories);
        assertTrue(categories.contains("fiction"));

        JsonElement obj = context.read("$.books[0].category");//fixme string array 多种类型的支撑
        log.debug(obj);

        context.set("$.books[0].category","f0");

//        categories.forEach(a1->System.out.println(a1));



    }


}
