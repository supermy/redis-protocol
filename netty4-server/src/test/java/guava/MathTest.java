package guava;

import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by moyong on 2017/11/1.
 */
public class MathTest {

    @Test
    public  void math() throws Exception {


        Assert.assertEquals(Math.decrementExact(1),0);
        Assert.assertEquals(Math.decrementExact(0),-1);
        Assert.assertEquals(Math.incrementExact(0),1);
        Assert.assertEquals(Math.incrementExact(-1),0);

        long[] ints = {1, 2, 3};
        Assert.assertEquals(Longs.asList(ints).size(),3);

        

    }
}
