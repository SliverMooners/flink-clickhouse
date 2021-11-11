package com.flink.stream.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author fanc
 * @date 2021/11/9 8:06 下午
 * @Description 流处理
 */
public class SocketStreamWordCount {

    public static void main(String[] args) throws Exception{

        // 1. 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据 nc
        // DataStream<String> inputDataSet = env.socketTextStream("127.0.0.1",7777);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataSet = env.socketTextStream(host,port);

        // 3. 处理数据集 按照空格切割, (word 单词 ,1 次数) 元组类型进行统计
        DataStream<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatMapper())
                .keyBy(0).sum(1);

        // 4> (你好,1)   打印的 5> 其实是线程数
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
