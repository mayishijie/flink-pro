package com.mayishijie.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author tty
 * @version 1.0 2021-06-18 12:47
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("/Users/tty/IdeaProjects/spark/flink-pro/flink-java/src/main/resources/wc.txt");
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(new Tuple2<String, Integer>(s2, 1));
                }
            }
        }).groupBy(0).sum(1);


        aggregateOperator.print();
    }
}
