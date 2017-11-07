package guava;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by moyong on 2017/11/4.
 */
public class TestList {
    static final int N=50000;
    static long timeList(List list){
        long start=System.currentTimeMillis();
        Object o = new Object();
        for(int i=0;i<N;i++) {
            list.add(0, o);
        }
        return System.currentTimeMillis()-start;
    }
    static long readList(List list){
        long start=System.currentTimeMillis();
        for(int i=0,j=list.size();i<j;i++){

        }
        return System.currentTimeMillis()-start;
    }

    static List addList(List list){
        Object o = new Object();
        for(int i=0;i<N;i++) {
            list.add(0, o);
        }
        return list;
    }
    public static void main(String[] args) {
        System.out.println("ArrayList添加"+N+"条耗时："+timeList(new ArrayList()));
        System.out.println("LinkedList添加"+N+"条耗时："+timeList(new LinkedList()));

        List list1=addList(new ArrayList<>());
        List list2=addList(new LinkedList<>());
        System.out.println("ArrayList查找"+N+"条耗时："+readList(list1));
        System.out.println("LinkedList查找"+N+"条耗时："+readList(list2));

        LinkedList<Integer> ints = new LinkedList<Integer>();
        ints.add(1);
        ints.add(2);
        ints.add(3);
        ints.add(4);
        ints.add(5);
        ints.listIterator(1).previous();
        ints.listIterator(1).next();
        System.out.println(""+ints.listIterator(1).previous());
        System.out.println(""+ints.listIterator(1).next());
        System.out.println(""+ints.getFirst());
        System.out.println(""+ints.getLast());
        System.out.println(""+ints.get(1));
        System.out.println(""+ints.get(1));

    }

}

