package com.pcl.dataHandle;

public class asd {
    public static void main(String[] args) {

        System.out.println(System.currentTimeMillis());
        String temp = System.currentTimeMillis() + "";
        System.out.println(temp.substring(0,10) + "." + temp.substring(10,13));
    }
}
