package com.joey.hbase;

/**

 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 整个程序中，只使用一个HConnection实例,因为SimpleHTableInterfaceFactory本身就是单例的。
 * connection还采用了延迟加载。整个类都是线程安全的实现。
 *
 * @author joey.wen
 * @date 2015/1/5
 */

public class SimpleHTableInterfaceFactory implements HTableInterfaceFactory {

    private static SimpleHTableInterfaceFactory singleton = new SimpleHTableInterfaceFactory();

    public static HTablePool createPool(Configuration config) {
        return new HTablePool(config, Integer.MAX_VALUE, singleton);
    }

    private volatile HConnection connection;

    private SimpleHTableInterfaceFactory() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    try {
                        connection = HConnectionManager.createConnection(config);
                    } catch (ZooKeeperConnectionException e) {
                        throw new RuntimeException("init HConnection failed", e);
                    }
                }
            }
        }
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException("get table failed", e);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        table.close();
    }
}
