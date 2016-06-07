package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.Tuple5;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator applies inequality self join on elements of input datasets.
 */
public class IESelfJoinOperator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
        extends UnaryToUnaryOperator<Record, Tuple2<Record, Record>> {
    protected final int get0Pivot;
    protected final JoinCondition cond0;
    protected final int get0Ref;
    protected final JoinCondition cond1;
    protected boolean list1ASC;
    protected boolean list1ASCSec;
    protected boolean list2ASC;
    protected boolean list2ASCSec;
    protected boolean equalReverse;

    public IESelfJoinOperator(Class<Record> inputTypeClass,
                              int get0Pivot, JoinCondition cond0,
                              int get0Ref, JoinCondition cond1) {
        super(DataSetType.createDefault(inputTypeClass),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false, null);
        this.get0Pivot = get0Pivot;
        this.cond0 = cond0;
        this.get0Ref = get0Ref;
        this.cond1 = cond1;
        assignSortOrders();
    }

    public IESelfJoinOperator(DataSetType<Record> inputType,
                              int get0Pivot, JoinCondition cond0,
                              int get0Ref, JoinCondition cond1) {
        super(inputType, DataSetType.createDefaultUnchecked(Tuple2.class), false, null);
        this.get0Pivot = get0Pivot;
        this.cond0 = cond0;
        this.get0Ref = get0Ref;
        this.cond1 = cond1;
        assignSortOrders();
    }

    public void assignSortOrders() {
        Tuple5<Boolean, Boolean, Boolean, Boolean, Boolean> sortOrders = IEJoinMasterOperator.getSortOrders(this.cond0, this.cond1);
        list1ASC = sortOrders.getField0();
        list1ASCSec = sortOrders.getField1();
        list2ASC = sortOrders.getField2();
        list2ASCSec = sortOrders.getField3();
        equalReverse = sortOrders.getField4();
    }
}
