package org.apache.wayang.basic.data;

import org.apache.wayang.core.types.DataUnitType;

import java.util.List;
import java.util.Map;

/**
 * This class can describe the structure of a Row in a DataFrame.
 */
public record Schema(List<Map.Entry<String, DataUnitType<Row>>> schema) {

    public int getIndexOfAttribute(String columnName) {
        if (columnName == null || schema == null) {
            return -1;
        }
        int size = schema.size();
        for (int i = 0; i < size; i++) {
            if (schema.get(i).getKey().equals(columnName)) {
                return i;
            }
        }
        return -1; // Not found, of course in a real implementation an exception would be thrown
    }

}
