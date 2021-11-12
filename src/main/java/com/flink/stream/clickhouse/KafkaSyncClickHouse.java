package com.flink.stream.clickhouse;

import com.alibaba.fastjson.JSON;
import com.flink.stream.clickhouse.domain.ClickHouseUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author fanc
 *
 * 进入clickhouse-client
 * use default;
 * drop table if exists user_table;
 * CREATE TABLE fanc.user_table(id UInt16, name String, age UInt16 ) ENGINE = TinyLog();
 *
 */
@Slf4j
public class KafkaSyncClickHouse {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 内核数
        env.setParallelism(Runtime.getRuntime().availableProcessors());

        // kafka
        //设置监控数据流的时间间隔, 执行会打印日志
        env.enableCheckpointing(10000);
        //配置KafKa和Zookeeper的ip和端口
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test-consumer-group");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("chart", new SimpleStringSchema(),
                properties);

        //添加数据源，此处选用数据流的方式，将KafKa中的数据转换成Flink的DataStream类型
        DataStream<String> stream = env.addSource(myConsumer);
        SingleOutputStreamOperator<ClickHouseUser> dataStream = stream.map(new MapFunction<String, ClickHouseUser>() {
            @Override
            public ClickHouseUser map(String data) throws Exception {
                String[] split = data.split(",");

                ClickHouseUser clickHouseUser = ClickHouseUser.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
                log.info("-----------: {}", JSON.toJSONString(clickHouseUser));
                return clickHouseUser;
            }
        });

        dataStream.print();
        //执行Job，Flink执行环境必须要有job的执行步骤，而以上的整个过程就是一个Job
        env.execute("WordCount From KafKa data");
    }
}