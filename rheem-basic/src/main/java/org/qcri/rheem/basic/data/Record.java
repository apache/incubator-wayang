package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
 */
public class Record implements Serializable {

    private Object[] values;

    private RecordSchema schema;

    public RecordSchema getSchema() {
        return schema;
    }

    public Record(Object... values) {
        String[] names = new String[values.length];
        Class[] types = new Class[values.length];
        this.values = values;
        for (Integer i=0; i <values.length; i++){
            names[i] = String.format("field%s", i);
            types[i] = values[i].getClass();
        }
        this.schema = new RecordSchema(names, types);

    }

    public Record(String[] names, Object[] values) {
        assert names.length == values.length;
        this.values = values;
        Class [] types = new Class[values.length];
        for (Integer i=0; i <values.length; i++){
            types[i] = values[i].getClass();
        }
        this.schema = new RecordSchema(names, types);

    }

    public Record(RecordSchema schema, Object[] values) {
        this.values = values;
        this.schema = schema;
    }

    public Record copy() {
        Object[] values_c = new Object[values.length];
        System.arraycopy(values, 0, values_c, 0, values.length);
        return new Record(schema.copy(), values_c);
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
        return Arrays.toString(values);
    }

    public Object getField(Integer index) {
        return values[index];
    }

    public Object getField(String name) {
        return values[schema.getFieldIndex(name)];
    }

    public String getFieldName(Integer index) {
        return schema.getFieldName(index);
    }

    public Class getFieldType(Integer index) {
        return schema.getFieldType(index);
    }

    public Class getFieldType(String name) {
        return schema.getFieldType(name);
    }

}
