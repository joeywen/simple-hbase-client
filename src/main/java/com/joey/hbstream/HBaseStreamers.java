package com.joey.hbstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * @author joey.wen
 * @date 2015/1/6
 *
 * reference by http://blog.genuine.com/2013/06/streaming-real-time-data-into-hbase/
 */
public class HBaseStreamers {
    private Configuration hbaseConfig;
    private Streamer[] streamers;
    private boolean started = false;

    private class Streamer implements Runnable {
        private LinkedBlockingQueue<Put> queue;
        private HTable table;
        private String tableName;
        private int counter = 0;

        public Streamer(String tableName, boolean autoFlush, int capacity) throws Exception {
            table = new HTable(hbaseConfig, tableName);
            table.setAutoFlush(autoFlush);
            this.tableName = tableName;
            queue = new LinkedBlockingQueue<Put>(capacity);
        }

        public void run() {
            while (true) {
                try {
                    Put put = queue.take();
                    table.put(put);
                    counter++;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void write(Put put) throws Exception {
            queue.put(put);
        }

        public void flush() {
            if (!table.isAutoFlush()) {
                try {
                    table.flushCommits();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public int size() {
            return queue.size();
        }

        public int counter() {
            return counter;
        }
    }

    public HBaseStreamers(String quorum, String port, String tableName, boolean autoFlush, int numOfStreamers, int capacity) throws Exception {
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", quorum);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
        streamers = new Streamer[numOfStreamers];
        for (int i = 0; i < streamers.length; i++) {
            streamers[i] = new Streamer(tableName, autoFlush, capacity);
        }
    }

    public Runnable[] getStreamers() {
        return streamers;
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        int count = 1;
        for (Streamer streamer : streamers) {
            new Thread(streamer, streamer.tableName + " HBStreamer " + count).start();
            count++;
        }
    }

    public void write(Put put) throws Exception {
        int i = (int) (System.currentTimeMillis() % streamers.length);
        streamers[i].write(put);
    }

    public void flush() {
        for (Streamer streamer : streamers) {
            streamer.flush();
        }
    }

    public int size() {
        int size = 0;
        for (Streamer st : streamers) {
            size += st.size();
        }
        return size;
    }

    public int counter() {
        int counter = 0;
        for (Streamer st : streamers) {
            counter += st.counter();
        }
        return counter;
    }
}
