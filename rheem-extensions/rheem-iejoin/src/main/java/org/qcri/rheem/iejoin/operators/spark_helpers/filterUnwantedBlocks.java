package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.iejoin.operators.IEJoinMasterOperator;
import scala.Tuple2;
import scala.Tuple5;

/**
 * Created by khayyzy on 5/28/16.
 */
public class filterUnwantedBlocks<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
        implements
        Function<Tuple2<Tuple5<Long, Type0, Type0, Type1, Type1>, Tuple5<Long, Type0, Type0, Type1, Type1>>, Boolean> {

    IEJoinMasterOperator.JoinCondition c1;
    boolean list2ASC;

    public filterUnwantedBlocks(IEJoinMasterOperator.JoinCondition c1, boolean list2ASC) {
        this.c1 = c1;
        this.list2ASC = list2ASC;
    }

    @SuppressWarnings("unchecked")
    public Boolean call(
            Tuple2<Tuple5<Long, Type0, Type0, Type1, Type1>, Tuple5<Long, Type0, Type0, Type1, Type1>> in)
            throws Exception {
        boolean test1 = compare(in._1()._2(), in._2()._3());
        boolean test2 = compareMinMax(in._1()._4(), in._1()._5(), in._2()._4(),
                in._2()._5());
        return (test1 && test2);
    }

    private boolean compareMinMax(Type1 min1, Type1 max1, Type1 min2, Type1 max2) {
        if (list2ASC) {
            //return !(max1 < min2);
            return (max1.compareTo(min2) >= 0);
        } else {
            //return !(min1 > max2);
            return (min1.compareTo(max2) <= 0);
        }
    }

    private boolean compare(Type0 d1, Type0 d2) {
        int i = d1.compareTo(d2);
        if (this.c1 == IEJoinMasterOperator.JoinCondition.GreaterThan) {//this.c1.equals(">")) {
            return (i > 0);
        } else if (this.c1 == IEJoinMasterOperator.JoinCondition.GreaterThanEqual) {//this.c1.equals(">=")) {
            return (i >= 0);
        } else if (this.c1 == IEJoinMasterOperator.JoinCondition.LessThan) {//this.c1.equals("<")) {
            return (i < 0);
        } else if (this.c1 == IEJoinMasterOperator.JoinCondition.LessThanEqual) {//this.c1.equals("<=")) {
            return (i <= 0);
        } else
            return false;
    }

}
