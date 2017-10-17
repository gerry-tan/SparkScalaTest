package com.tan.designMode;

/**
 * 饿汉式
 * Created by Tan on 2017/9/15.
 */
public class Singelton {
    private Singelton(){}

    private static final Singelton singelton = new Singelton();

    public static Singelton getInstance(){
        return singelton;
    }
}

/**
 * 懒汉式
 */
class Sigelton2 {

    private Sigelton2() {}

    private static Sigelton2 sigelton2 = null;

    public static Sigelton2 getInstance() {
        if (sigelton2 == null) {
            sigelton2 = new Sigelton2();
        }
        return sigelton2;
    }

}
