package org.qcri.rheem.iejoin.operators.spark_helpers;


import org.qcri.rheem.iejoin.data.Data;

import java.io.Serializable;
import java.util.Comparator;

public class revDataComparator implements Serializable, Comparator<Data> {

    private static final long serialVersionUID = 1L;

    boolean asc1;
    boolean asc2;
    boolean revRowID;

    public revDataComparator(boolean asc1, boolean asc2, boolean revRowID) {
        this.asc1 = asc1;
        this.asc2 = asc2;
        this.revRowID = revRowID;
    }

    public int compare(Data o1, Data o2) {
        int dff = 0;
        if (asc1) {
            dff = o1.compareRank(o2);
        } else {
            dff = o2.compareRank(o1);
        }
        if (dff == 0) {
            int dff2 = 0;
            if (asc2) {
                dff2 = o1.compareTo(o2);
            } else {
                dff2 = o2.compareTo(o1);
            }
            // third level of sorting
            if (dff2 == 0) {
                if ((o1.isPivot() && o2.isPivot())
                        || (!o1.isPivot() && !o2.isPivot())) {
                    if (!revRowID) {
                        return ((int) o1.getRowID() - (int) o2.getRowID());
                    } else {
                        return ((int) o2.getRowID() - (int) o1.getRowID());
                    }
                } else if (o1.isPivot() && !revRowID) {
                    if (asc1) {
                        return 1;
                    } else {
                        return -1;
                    }
                } else if (o2.isPivot() && !revRowID) {
                    if (!asc1) {
                        return -1;
                    } else {
                        return 1;
                    }
                } else {
                    if (asc1) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
            return dff2;
        }
        return dff;
    }
}
