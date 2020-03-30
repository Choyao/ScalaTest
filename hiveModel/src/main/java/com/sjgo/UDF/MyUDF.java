package com.sjgo.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyUDF extends UDF {

    public String evaluate(String line){
        try{
            return line+ System.currentTimeMillis();
        }catch (Exception e){
            e.printStackTrace();
            return "ERROR";

        }
    }
}
