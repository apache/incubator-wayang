package org.qcri.rheem.basic.data;

import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple2<T0, T1> {

    public T0 field0;

    public T1 field1;

    public Tuple2() { }

    public Tuple2(T0 field0, T1 field1) {
        this.field0 = field0;
        this.field1 = field1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
        return Objects.equals(field0, tuple2.field0) &&
                Objects.equals(field1, tuple2.field1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field0, field1);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", this.field0, this.field1);
    }
}
