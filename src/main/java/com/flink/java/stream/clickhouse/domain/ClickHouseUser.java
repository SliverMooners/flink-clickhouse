package com.flink.java.stream.clickhouse.domain;

/**
 * @author fanc
 * demo
 */
public class ClickHouseUser {
    public int id;
    public String name;
    public int age;

    public ClickHouseUser(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public static ClickHouseUser of(int id, String name, int age) {
        return new ClickHouseUser(id, name, age);
    }

}