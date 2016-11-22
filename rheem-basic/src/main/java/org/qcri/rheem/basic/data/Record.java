package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.util.Copyable;
import org.qcri.rheem.core.util.ReflectionUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
 */
public class Record implements Serializable, Copyable<Record> {

    private Object[] values;

    public Record(Object... values) {
        this.values = values;
    }

    @Override
    public Record copy() {
        return new Record(this.values.clone());
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

    /**
     * Retrieve a field as a {@code double}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code double} representation of the field
     */
    public double getDouble(int index) {
        Object field = this.values[index];
        return ReflectionUtils.toDouble(field);
       }

    /**
     * Retrieve a field as a {@code long}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code long} representation of the field
     */
    public long getLong(int index) {
        Object field = this.values[index];
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Long) return (Long) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as long.", field));
    }

    /**
     * Retrieve a field as a {@code int}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code int} representation of the field
     */
    public int getInt(int index) {
        Object field = this.values[index];
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as int.", field));
    }

    /**
     * Retrieve a field as a {@link String}.
     *
     * @param index the index of the field
     * @return the field as a {@link String} (obtained via {@link Object#toString()}) or {@code null} if the field is {@code null}
     */
    public String getString(int index) {
        Object field = this.values[index];
        return field == null ? null : field.toString();
    }

    /**
     * Retrieve the size of this instance.
     *
     * @return the number of fields in this instance
     */
    public int size() {
        return this.values.length;
    }

}
