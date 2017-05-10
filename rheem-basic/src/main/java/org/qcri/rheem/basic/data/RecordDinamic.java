package org.qcri.rheem.basic.data;

import org.qcri.rheem.core.util.Copyable;
import org.qcri.rheem.core.util.ReflectionUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A Type that represents a record with a schema flexible, in where the value is associate a name.
 */
public class RecordDinamic implements Serializable, Copyable<RecordDinamic> {

    public static boolean statistic = false;

    private Map<String, Object> values;

    private String[] keys;

    private Tuple2<Integer, Integer> uses;

    public RecordDinamic(String... keys) {
        if(keys != null && keys.length > 0) {
            this.values = new HashMap<>(keys.length);
            for(String key: keys){
                this.values.put(key, null);
            }
            this.keys = keys;
        }else{
            this.values = new HashMap<>();
            this.keys = null;
        }
        uses =null;
    }

    public RecordDinamic(Map<String, Object> values, String[] keys){
        this.values = values;
        this.keys   = keys;
    }

    @Override
    public RecordDinamic copy() {
        return new RecordDinamic((Map)((HashMap)this.values).clone(), this.keys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        RecordDinamic record2 = (RecordDinamic) o;
        return this.values.equals(record2.getValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.values.hashCode());
    }

    @Override
    public String toString() {
        String used = "";
        if(statistic && this.uses != null){
            used = this.uses.toString();
        }
        return "RecordDinamic"+ used + this.values.toString();
    }

    public Object getField(String key) {
        return this.values.get(key);
    }

    public Map getValues(){
        return this.values;
    }

    /**
     * set the value for the key
     *
     * @param key the key that indentify the field
     * @param element is the element associated to the key
     */
    public void setValue(String key, Object element){
        if(this.keys == null){
            if(!this.values.containsKey(key)){
                this.values.put(key, element);
                return;
            }
        }
        this.values.replace(key, element);
    }

    /**
     * Retrieve a field as a {@code double}. It must be castable as such.
     *
     * @param key the key that indentify the field
     * @return the {@code double} representation of the field
     */
    public double getDouble(String key) {
        Object field = this.values.get(key);
        return ReflectionUtils.toDouble(field);
    }

    /**
     * Retrieve a field as a {@code long}. It must be castable as such.
     *
     * @param key the key that indentify the field
     * @return the {@code long} representation of the field
     */
    public long getLong(String key) {
        Object field = this.values.get(key);
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Long) return (Long) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as long.", field));
    }

    /**
     * Retrieve a field as a {@code int}. It must be castable as such.
     *
     * @param key the key that indentify the field
     * @return the {@code int} representation of the field
     */
    public int getInt(String key) {
        Object field = this.values.get(key);
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as int.", field));
    }

    /**
     * Retrieve a field as a {@link String}.
     *
     * @param key the key that indentify the field
     * @return the field as a {@link String} (obtained via {@link Object#toString()}) or {@code null} if the field is {@code null}
     */
    public String getString(String key) {
        Object field = this.values.get(key);
        return field == null ? null : field.toString();
    }

    /**
     * Retrieve the size of this instance.
     *
     * @return the number of fields in this instance
     */
    public int size() {
        return this.values.size();
    }

    /**
     * set the value for statistic of the record
     *
     * @param value is a element that describe a value
     * @param max is the element describe the maximum value that take the value
     */
    public void setUse(int value, int max){
        this.uses = new Tuple2<>(value, max);
    }
}