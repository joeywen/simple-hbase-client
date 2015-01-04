package com.joey.hbase;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Non-thread-safe, Use threadLocal to wrapper this class in multi-thread
 * @author joey.wen
 * @date 2014/12/30
 */
public final class HBaseCLientController {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseCLientController.class);

    private static Configuration DEFAULT_CONFIG = null;//
    private final static String HBASE_RPC_ENGINE="org.apache.hadoop.hbase.ipc.SecureRpcEngine";
    private final static String ZOOKEEPER_ZNODE_PARENT="/hbase";

    // private static Map<String, RowEntry> rowEntryMap = Maps.newConcurrentMap();
    private static LinkedList<Map<String, List<RowEntry>>> rotateCache = new LinkedList<Map<String, List<RowEntry>>>();

    protected static HConnection getConnection() throws ZooKeeperConnectionException {
        return getConnection(DEFAULT_CONFIG);
    }

    protected static HConnection getConnection(Configuration config) throws ZooKeeperConnectionException {
        if (config == null) {
            // use default configuration
            config = initConfiguration();
        }

        return HConnectionManager.getConnection(config);
    }

    protected static Map<String, List<RowEntry>> first() {
        return rotateCache.peek();
    }

    protected static void offer(Map<String, List<RowEntry>> cache) {
        rotateCache.offer(cache);
    }

    private boolean commit() {
        Map<String, List<RowEntry>> cache = rotateCache.poll();
        // add new map to rotateCache
        rotateCache.offer(new HashMap<String, List<RowEntry>>());

        Iterator<Map.Entry<String, List<RowEntry>>> iterator = cache.entrySet().iterator();
        String tableName;
        while (iterator.hasNext()) {
            Map.Entry<String, List<RowEntry>> entry = iterator.next();
            tableName = entry.getKey();
            try {
                getConnection().getTable(tableName).batch(optimize(entry.getValue()));
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error("", e);
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOG.error("", e);
            }
        }
        return true;
    }


    private List<Row> generateRowList(Map<Integer, RowEntry> commandMap) {
        List<Row> cmds = new ArrayList<Row>();
        Map<Integer, Row> results = Maps.newHashMap();
        Iterator<Map.Entry<Integer, RowEntry>> iterator = commandMap.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<Integer, RowEntry> mapEntry = iterator.next();
            RowEntry entry = mapEntry.getValue();

            Row row= results.get(entry.hashCode());
            if (row == null) {
                switch(entry.getType()) {
                    case PUT:
                        row = new Put(Bytes.toBytes(entry.getRowKey()));
                        for (Column col : entry.getColumns()) {
                            ((Put)row).add(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()), col.getValue());
                        }
                        break;
                    case INCREMENT:
                        row = new Increment(Bytes.toBytes(entry.getRowKey()));
                        for (Column col : entry.getColumns()) {
                            ((Increment)row).addColumn(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()), Bytes.toLong(col.getValue()));
                        }
                        break;
                }
            } else {
                switch (entry.getType()) {
                    case PUT: // TODO 考虑值覆盖情况 ？？？？
                        for (Column col : entry.getColumns()) {
                            ((Put)row).add(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()), col.getValue());
                        }
                        break;
                    case INCREMENT:
                        for (Column col : entry.getColumns()) {
                            ((Increment)row).addColumn(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()), Bytes.toLong(col.getValue()));
                        }
                        break;
                }
            }

            results.put(entry.hashCode(), row);
        }

        cmds.addAll(results.values());
        return cmds;
    }

    private List<? extends Row> optimize(List<RowEntry> entries) {
        Map<Integer, RowEntry> commandMap = new ConcurrentHashMap<Integer, RowEntry>();

        for (final RowEntry rowEntry : entries) {
            if (rowEntry == null) {
                continue;
            }

            /**
             * RowEntry 的hashCode是tableName，rowKey，family，qualifier，opttype， isOverrideTime 组成，算是唯一
             * 如果commandMap含有该put记录，那么后续相同的操作将被抛弃掉；
             * 如果commandMap含有该incr记录，合并其value值
             * **/
            if (commandMap.containsKey(rowEntry.hashCode())) {
                RowEntry e = commandMap.get(rowEntry.hashCode());
                if (e == null) {
                    commandMap.put(rowEntry.hashCode(), rowEntry);
                    continue;
                }

                if (e.getType() != RowEntry.WriteCmdType.INCREMENT) {
                    continue;
                }
                List<Column> ll = new ArrayList<Column>();
                for (Column col : rowEntry.getColumns()) {
                    for (Column col2 : e.getColumns()) {
                        if (col.isSame(col2)) {
                            long val = Bytes.toLong(col.getValue());
                            long cur = Bytes.toLong(col2.getValue());
                            col.setValue(Bytes.toBytes(val + cur));
                        }
                    }
                    ll.add(col);
                }
                rowEntry.setColumns(ll.toArray(new Column[0]));
            } else  { // 添加新的操作
                commandMap.put(rowEntry.hashCode(), rowEntry);
            }
        }

        return generateRowList(commandMap);
    }


    private static Configuration initConfiguration() {
        DEFAULT_CONFIG = HBaseConfiguration.create();

        String quorum = ConfigLoader.get("hbase.zookeeper.quorum");
        String clientPort = ConfigLoader.get("hbase.zookeeper.property.clientPort");
        if (StringUtils.isEmpty(quorum) || StringUtils.isEmpty(clientPort)) {
            throw new NullPointerException("HBase zookeeper quorum or client port is null. quorum[" + quorum + "]clientport[" + clientPort + "]");
        }

        DEFAULT_CONFIG.set("hbase.zookeeper.quorum", quorum);
        DEFAULT_CONFIG.set("hbase.zookeeper.property.clientPort", clientPort);
        DEFAULT_CONFIG.set("hbase.rpc.engine",HBASE_RPC_ENGINE);

        String znodeParent = ConfigLoader.get("zookeeper.znode.parent");
        DEFAULT_CONFIG.set("zookeeper.znode.parent",znodeParent == null ? ZOOKEEPER_ZNODE_PARENT : znodeParent);

        return DEFAULT_CONFIG;
    }
}
