package com.joey.hbase;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author joey.wen
 * @date 2014/12/29
 */
public final class RowEntry implements Serializable {
    private String tableName;
    private String rowKey;
    private Column[] columns;
    private WriteCmdType type = WriteCmdType.UNKONOWN;


    public static enum WriteCmdType {
        PUT,
        INCREMENT,
        UNKONOWN
    }

    public RowEntry(String tableName, String rowKey, String family, String qualifier, byte[] value, WriteCmdType type) {
        this(tableName, rowKey, type, new Column(family, qualifier, value));
    }

    // TODO constructor use variant parameter ????
    public RowEntry(String tableName, String rowKey, WriteCmdType type, Column... columns) {
        this.tableName = tableName;
        this.rowKey = rowKey;
        this.type = type;
        this.columns = columns;
    }

    @Override
    public int hashCode() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(tableName.hashCode()).append(rowKey.hashCode()).append(type.hashCode());
        return sb.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowEntry) return false;
        RowEntry o = (RowEntry) obj;
        return (getRowKey().equals(o.getRowKey()))
                && (getType() == o.getType())
                && (getTableName().equals(o.getTableName()));
    }

    @Override
    public String toString() {
        return "RowEntry{" +
                "tableName='" + tableName + '\'' +
                ", rowKey='" + rowKey + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", type=" + type +
                '}';
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public WriteCmdType getType() {
        return type;
    }

    public void setType(WriteCmdType type) {
        this.type = type;
    }
}
