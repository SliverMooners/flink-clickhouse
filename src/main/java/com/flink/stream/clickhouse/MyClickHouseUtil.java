package com.flink.stream.clickhouse;

import com.alibaba.fastjson.JSON;
import com.flink.stream.clickhouse.domain.ClickHouseUser;
import com.flink.utils.ClickHouseUtil;
import jdk.nashorn.internal.runtime.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


/**
 * @author fanc
 */
@Slf4j
public class MyClickHouseUtil extends RichSinkFunction<ClickHouseUser> {
    Connection connection = null;

    String sql;

    public MyClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("MyClickHouseUtil parameters: {}", JSON.toJSONString(parameters));
        String host = parameters.getString("host","10.24.19.31");
        int port = parameters.getInteger("port",8123) ;
        String database = parameters.getString("database","fanc");
        connection = ClickHouseUtil.getConn(host, port, database);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(ClickHouseUser user, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, user.id);
        preparedStatement.setString(2, user.name);
        preparedStatement.setLong(3, user.age);
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}