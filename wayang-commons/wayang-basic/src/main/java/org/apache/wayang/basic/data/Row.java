package org.apache.wayang.basic.data;

import java.util.List;

/**
 * Represents a single row of data with an associated schema.
 * @param schema The metadata describing the structure of the fields.
 */
public record Row(List<Object> fields, Schema schema) {

    public int size() {
        return fields != null ? fields.size() : 0;
    }

    public Object getAttribute(String attribute) {
        int indexOfAttribute = schema.getIndexOfAttribute(attribute);
        if (indexOfAttribute >= 0) {
            return this.getAttributeAtIndex(indexOfAttribute);
        }
        return null;
    }

    private Object getAttributeAtIndex(int index) {
        return fields != null ? fields.get(index) : null;
    }
}