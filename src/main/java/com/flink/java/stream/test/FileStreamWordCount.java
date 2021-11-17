package com.flink.java.stream.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author fanc
 * @date 2021/11/9 8:06 下午
 * @Description 流处理
 */
public class FileStreamWordCount {

    public static void main(String[] args) throws Exception{

        // 1. 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取路径
        String filePath = FileStreamWordCount.class.getClassLoader().getResource("hello.txt").getPath();

        // 3. 读取数据
        DataStream<String> inputDataSet = env.readTextFile(filePath);

        // 4. 处理数据集 按照空格切割, (word 单词 ,1 次数) 元组类型进行统计
        DataStream<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatMapper())
                .keyBy(0).sum(1);

        // 5> (你好,1)   打印的 5> 其实是线程数
        sum.print();

        // 5. 执行任务
        env.execute("Flink Streaming Java API Skeleton");
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
