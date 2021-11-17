package com.flink.java.stream.clickhouse;

import com.flink.java.stream.clickhouse.domain.ClickHouseUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class ClickHouseSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform 操作
        SingleOutputStreamOperator<ClickHouseUser> dataStream = inputStream.map(new MapFunction<String, ClickHouseUser>() {
            @Override
            public ClickHouseUser map(String data) throws Exception {
                String[] split = data.split(",");
                return ClickHouseUser.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
            }
        });

        // sink
        String sql = "INSERT INTO fanc.user_table (id, name, age) VALUES (?,?,?)";
        MyClickHouseUtil jdbcSink = new MyClickHouseUtil(sql);
        dataStream.addSink(jdbcSink);
        dataStream.print();
        env.execute("clickhouse sink test");
    }
}