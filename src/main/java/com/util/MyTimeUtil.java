package com.util;

/**
 * @author maxwell
 * @Description: TODO
 * @date 2019/12/6 15:10
 */
public class MyTimeUtil {

    public static void sleep(long l){
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
