package org.qcri.rheem.basic.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Created by yidris on 4/10/16.
 */
public class RecordSchema implements Serializable {

    private String[] names;
    private Class [] types;

    public RecordSchema(String[] names, Class[] types){
        this.names = names;
        this.types = types;
    }

    public String[] getNames() {
        return names;
    }

    public Class[] getTypes() {
        return types;
    }

    public Integer getFieldIndex(String name) {
        return Arrays.asList(names).indexOf(name);
    }

    public String getFieldName(Integer index) {
        return names[index];
    }

    public Class getFieldType(Integer index) {
        return types[index];
    }

    public Class getFieldType(String name) {
        return types[getFieldIndex(name)];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        RecordSchema schema2 = (RecordSchema) o;
        return Arrays.equals(this.names, schema2.names) && Arrays.equals(this.types, schema2.types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(types), Arrays.hashCode(names));
    }

    @Override
    public String toString() {
        return String.format("Field Types: %s, Field names: %s", Arrays.toString(types), Arrays.toString(names));
    }

    public RecordSchema copy() {
        String[] names_c = new String[this.names.length];
        System.arraycopy(this.names, 0, names_c, 0, this.names.length);
        Class[] types_c = new Class[types.length];
        System.arraycopy(this.types, 0, types_c, 0, this.types.length);
        return new RecordSchema(names_c, types_c);
    }
}
