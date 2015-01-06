package com.joey.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author joey.wen
 * @date 2014/12/29
 */
public interface HBaseClientInterface {
    /** put methods **/
    public boolean put(final String tableName, final String rowKey, final String family, final String qualifier, final byte[] value);
    public boolean put(final String tableName, final String rowKey, final Column... columns);
    public boolean put(final RowEntry rowEntry);

    /** increment methods **/
    public boolean increment(final String tableName, final String rowKey, final String family, final String qualifier, final long value);
    public boolean increment(final String tableName, final String rowKey, final Column... columns);
    public boolean increment(final RowEntry rowEntry);

    /** get methods **/
    public Result get(final String tableName, final String rowKey);
    public Result get(final String tableName, final String rowKey, final String family);
    public Result get(final String tableName, final String rowKey, final String family, final String qualifier);


    /** delete methods **/
    public boolean delete(final String tableName, final String rowKey);
    public boolean delete(final String tableName, final String rowKey, final String family);
    public boolean delete(final String tableName, final String rowKey, final String family, final String qualifier);
    public boolean delete(final RowEntry rowEntry);

    /** getScanner methods**/
    public ResultScanner getScanner(final String tableName, final String family);
    public ResultScanner getScanner(final String tableName, final String family, final String qualifier);
    public ResultScanner getScanner(final String tableName, final Scan scan);

    public boolean commit();
}

