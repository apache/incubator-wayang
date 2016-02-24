package org.qcri.rheem.apps.tpch.data.q1;

import java.io.Serializable;
import java.util.Objects;

/**
 * Grouping key used in Query 1.
 */
public class GroupKey implements Serializable {

    public char L_RETURNFLAG;

    public char L_LINESTATUS;

    public GroupKey(char l_RETURNFLAG, char l_LINESTATUS) {
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_LINESTATUS = l_LINESTATUS;
    }

    public GroupKey() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return this.L_RETURNFLAG == groupKey.L_RETURNFLAG &&
                this.L_LINESTATUS == groupKey.L_LINESTATUS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.L_RETURNFLAG, this.L_LINESTATUS);
    }
}
