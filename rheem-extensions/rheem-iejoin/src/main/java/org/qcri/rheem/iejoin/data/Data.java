package org.qcri.rheem.iejoin.data;

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

    /**
     * Created by khayyzy on 5/28/16.
     */
    public static class Comparator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
            implements Serializable, java.util.Comparator<Data<Type0, Type1>> {

        private static final long serialVersionUID = 1L;

        boolean asc1;
        boolean asc2;

        public Comparator(boolean asc1, boolean asc2) {
            this.asc1 = asc1;
            this.asc2 = asc2;
        }

        public int compare(Data o1, Data o2) {
            // first level of sorting
            int dff = 0;
            if (asc1) {
                dff = o1.compareTo(o2);
            } else {
                dff = o2.compareTo(o1);
            }
            // second level of sorting
            if (dff == 0) {
                int dff2 = 0;
                if (asc2) {
                    dff2 = o1.compareRank(o2);
                } else {
                    dff2 = o2.compareRank(o1);
                }
                // third level of sorting
                if (dff2 == 0) {
                    if ((o1.isPivot() && o2.isPivot())
                            || (!o1.isPivot() && !o2.isPivot())) {
                        return ((int) o1.getRowID() - (int) o2.getRowID());
                    } else if (o1.isPivot()) {
                        if (asc1) {
                            return -1;
                        } else {
                            return 1;
                        }
                    } else if (o2.isPivot()) {
                        if (!asc1) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                }
                return dff2;
            }
            return dff;
        }
    }
}
