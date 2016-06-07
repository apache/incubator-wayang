package org.qcri.rheem.basic.data;

import java.io.Serializable;

/**
 * Created by khayyzy on 5/28/16.
 */
public class Data<Type0 extends Comparable<Type0>,
        Type1 extends Comparable<Type1>>
        implements Serializable, Comparable<Data<Type0, Type1>> {
    /**
     *
     */
    private static final long serialVersionUID = 2808795863775557984L;
    long rowID;
    Type0 value;
    Type1 rank;
    boolean pivot;

    public Data(long rowID, Type0 value, Type1 rank) {
        super();
        this.rowID = rowID;
        this.value = value;
        this.rank = rank;
        this.pivot = true;
    }

    public void resetPivot() {
        this.pivot = false;
    }

    public boolean isPivot() {
        return this.pivot;
    }

    public long getRowID() {
        return rowID;
    }

    public void setRowID(long inRow) {
        this.rowID = inRow;
    }

    public Type0 getValue() {
        return this.value;
    }

    public Type1 getRank() {
        return this.rank;
    }

    public int compareTo(Data<Type0, Type1> d) {
        return value.compareTo(d.getValue());
    }

    public int compareRank(Data<Type0, Type1> d) {
        return rank.compareTo(d.getRank());
    }

    public int compareTo(Type0 o) {
        return value.compareTo(o);
    }

    public String toString() {
        return rowID + ":" + pivot + ":{" + value.toString() + "-" + rank.toString() + "}";
    }
}
