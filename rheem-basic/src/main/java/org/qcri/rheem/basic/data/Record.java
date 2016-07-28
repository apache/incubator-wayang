package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
 */
public class Record implements Serializable {

    private Object[] values;

    public Record(Object... values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Record record2 = (Record) o;
        return Arrays.equals(this.values, record2.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(values));
    }

    @Override
    public String toString() {
        return "Record" + Arrays.toString(this.values);
    }

    public Object getField(int index) {
        return this.values[index];
    }

}
