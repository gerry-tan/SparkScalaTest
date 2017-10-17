package com.tan.designMode;

/**
 *  Created by Tan on 2017/9/15.
 */
interface Car {
    public void drive();
}

class BM implements Car {

    public void drive() {
        System.out.println("driving BM");
    }
}

class BC implements Car {
    public void drive() {
        System.out.println("driving BC");
    }
}

public class Factory {
    public static Car driveCar(String name) throws Exception{
        if (name.equals("BM")) {
            return new BM();
        }else if (name.equals("BC")) {
            return new BC();
        }
        return null;
    }

    public static void main(String[] args) throws Exception{
        Car car = Factory.driveCar("BM");
        car.drive();
    }
}
