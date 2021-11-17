package com.flink.java.batch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author fanc
 * @date 2021/11/9 12:55 下午
 * @Description 批处理有界 dataSet
 */
@Slf4j
public class FileWordCount {

    public static void main(String[] args) throws Exception{

        // 1. 获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取路径
        // String filePath = WordCount.class.getClassLoader().getResource("")+"/hello.txt";
        String filePath = FileWordCount.class.getClassLoader().getResource("hello.txt").getPath();

        // 3. 读取数据
        DataSet<String> inputDataSet = env.readTextFile(filePath);

        // 4. 处理数据集 按照空格切割, (word 单词 ,1 次数) 元组类型进行统计
        // groupBy(0).sum(1); 0 1 对应数据的位置
        DataSet<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0).sum(1);
        sum.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }

}
