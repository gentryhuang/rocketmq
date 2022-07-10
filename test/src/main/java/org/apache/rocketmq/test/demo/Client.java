package org.apache.rocketmq.test.demo;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

/**
 * Client
 *
 * @author <a href="mailto:libao.huang@yunhutech.com">shunhua</a>
 * @since 2021/11/20
 * <p>
 * desc：
 */
public class Client {
    public static void main(String[] args) {
        ArrayList<Integer> arrayList = new ArrayList();
        arrayList.add(1);
        arrayList.add(2);

        String str = "hello";



        // 将引用赋值给 integer
        for (Integer integer : arrayList){
            // 指向同一个内存地址，数据一致
            System.out.println(integer);

            //  integer 重新指向，和 arrayList 对应的引用 半毛钱关系都没有
            integer += 1;
            System.out.println(integer);
        }


        test(str);

        System.out.println(str);


        Random random = new Random();
        for (int i = 0;i <1000;i++) {
            long delayTimeMills = random.nextInt(1000 * 60*5);
            System.out.println(delayTimeMills);
        }




    }

    public static void test(String a){
        System.out.println(a);
        a = a + "11";

        System.out.println(a);
    }
}
