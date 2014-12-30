package com.joey.hbase;

import java.util.Arrays;

/**
 * @author joey.wen
 * @date 2014/12/29
 */
public class Column {
    private String family;
    private String qualifier;
    private byte[] value;

    public Column(String family, String qualifier, byte[] value) {
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public boolean isSame(Column col) {
        return (this.getFamily().equals(col.getFamily()) && this.getQualifier().equals(col.getQualifier()));
    }

    @Override
    public int hashCode() {
        return (this.family.hashCode() + "" + this.qualifier.hashCode()).hashCode();
    }

    @Override
    public String toString() {
        return "Columns{" +
                "family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", value=" + Arrays.toString(value) +
                '}';
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
