package com.joey.hbase;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Non-thread-safe, Use threadLocal to wrapper this class in multi-thread
 * @author joey.wen
 * @date 2014/12/29
 *
 */
public class HBaseClientImpl implements HBaseClientInterface {

    private final static Logger LOG = LoggerFactory.getLogger(HBaseClientImpl.class);

    @Override
    public boolean put(String tableName, String rowKey, String family, String qualifier, byte[] value) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(family)
                || StringUtils.isEmpty(rowKey) || StringUtils.isEmpty(qualifier)) {
            LOG.error(String.format("one of these val is null, tableName=%s, rowKey=%s,family=%s,qualifier=%s",
                    tableName, rowKey, family, qualifier));
            return false;
        }
        return put(tableName, rowKey, new Column(family, qualifier, value));
    }

    @Override
    public boolean put(String tableName, String rowKey, Column... columns) {
        Validate.noNullElements(columns);
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(rowKey)) {
            LOG.error(String.format("one of these val is null, tableName=%s, rowKey=%s",
                    tableName, rowKey));
            return false;
        }

        return put(new RowEntry(tableName, rowKey, RowEntry.WriteCmdType.PUT, columns));
    }

    @Override
    public boolean put(final RowEntry rowEntry) {
        return _writeCommand(rowEntry);
    }

    private boolean _writeCommand(final RowEntry rowEntry) {
        Map<String, List<RowEntry>> cache = HBaseCLientController.first();
        if (cache == null) {
            cache = Maps.newConcurrentMap();
            HBaseCLientController.offer(cache);
            cache.put(rowEntry.getTableName(), new ArrayList<RowEntry>(){{
                add(rowEntry);
            }});
            return true;
        }

        List<RowEntry> list = cache.get(rowEntry.getTableName());
        list.add(rowEntry);

        return true;
    }

    @Override
    public boolean increment(String tableName, String rowKey, String family, String qualifier, long value) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(family)
                || StringUtils.isEmpty(rowKey) || StringUtils.isEmpty(qualifier)) {
            LOG.error(String.format("one of these val is null, tableName=%s, rowKey=%s,family=%s,qualifier=%s",
                    tableName, rowKey, family, qualifier));
            return false;
        }
        return increment(tableName, rowKey, new Column(family, qualifier, Bytes.toBytes(value)));
    }

    @Override
    public boolean increment(String tableName, String rowKey, Column... columns) {
        Validate.noNullElements(columns);
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(rowKey)) {
            LOG.error(String.format("one of these val is null, tableName=%s, rowKey=%s",
                    tableName, rowKey));
            return false;
        }
        return increment(new RowEntry(tableName, rowKey, RowEntry.WriteCmdType.INCREMENT, columns));
    }

    @Override
    public boolean increment(RowEntry rowEntry) {
        return _writeCommand(rowEntry);
    }

    @Override
    public Result get(String tableName, String rowKey) {
        return get(tableName, rowKey, null);
    }

    @Override
    public Result get(String tableName, String rowKey, String family) {
        return get(tableName, rowKey, family, null);
    }

    @Override
    public Result get(String tableName, String rowKey, String family, String qualifier) {
        if (StringUtils.isEmpty(tableName)) {
            LOG.error(String.format("one of these val is null, tableName=%s", tableName));
            return null;
        }

        HConnection connection = null;
        try {
            connection = HBaseCLientController.getConnection();
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!StringUtils.isEmpty(family)) {
                if (StringUtils.isEmpty(qualifier)) {
                    get.addFamily(Bytes.toBytes(family));
                } else {
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                }
            }
            return connection.getTable(Bytes.toBytes(tableName)).get(get);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            LOG.error("ZooKeeperConnectionException ERROR: ", e);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("IOException ERROR: ", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Close HBase connection error. ", e);
                }
            }
        }

        return null;
    }

    @Override
    public boolean delete(String tableName, String rowKey) {
        return delete(tableName, rowKey, null);
    }

    @Override
    public boolean delete(String tableName, String rowKey, String family) {
        return delete(tableName, rowKey, family, null);
    }

    @Override
    public boolean delete(String tableName, String rowKey, String family, String qualifier) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(rowKey) ) {
            LOG.error(String.format("one of these val is null, tableName=%s, rowKey=%s",
                    tableName, rowKey));
            return false;
        }
        HConnection connection = null;
        try {
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (!StringUtils.isEmpty(family)) {
                if (!StringUtils.isEmpty(qualifier)) {
                    del.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                } else {
                    del.deleteFamily(Bytes.toBytes(family));
                }
            }
            connection.getTable(Bytes.toBytes(tableName)).delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Delete rowkey error. ", e);
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Close HBase connection error. ", e);
                }
            }
        }
    }

    @Override
    public boolean delete(RowEntry rowEntry) {
        return false;
    }

    @Override
    public ResultScanner getScanner(String tableName, String family) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(family)) {
            LOG.error("table name or family name is null, tableName[" + tableName + "] ,family[" + family + "]");
            return null;
        }
        return getScanner(tableName, new Scan().addFamily(Bytes.toBytes(family)));
    }

    @Override
    public ResultScanner getScanner(String tableName, String family, String qualifier) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(family) || StringUtils.isEmpty(qualifier)) {
            LOG.error("table name or family name is null, tableName[" + tableName +
                    "] ,family[" + family + "], qualifier[" + qualifier + "]");
            return null;
        }
        return getScanner(tableName, new Scan().addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
    }

    @Override
    public ResultScanner getScanner(String tableName, Scan scan) {
        HTableInterface htable = null;
        try {
            htable = HBaseCLientController.getConnection().getTable(Bytes.toBytes(tableName));
            return htable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Get scanner occur exception .", e);
            return null;
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
