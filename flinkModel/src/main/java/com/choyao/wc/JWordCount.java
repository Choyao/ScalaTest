package com.choyao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class JWordCount {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment jenv = ExecutionEnvironment.getExecutionEnvironment();
        final DataSource<String> word = jenv.readTextFile("C:\\Users\\ADMIN\\IdeaProjects\\ScalaTest\\flinkModel\\src\\main\\resources\\hello.txt");
        word.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] l = s.split(" ");
                for (String word : l) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).groupBy(0).sum(1).print();


    }

}
