package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
 */
public class Record implements Serializable {

    public Object[] fields;

    public Record() {
    }

    public Record(Object... fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Record record2 = (Record) o;
        return Arrays.equals(this.fields, record2.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(fields));
    }

    @Override
    public String toString() {
        return Arrays.toString(fields);
    }

}
