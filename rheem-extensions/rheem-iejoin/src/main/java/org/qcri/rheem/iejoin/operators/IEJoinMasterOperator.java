package org.qcri.rheem.iejoin.operators;

import org.qcri.rheem.basic.data.Tuple5;

/**
 * This operator decides the correct sorting orders for IEJoin
 */
public class IEJoinMasterOperator {
    public static Tuple5<Boolean, Boolean, Boolean, Boolean, Boolean> getSortOrders(JoinCondition cond0, JoinCondition cond1) {
        boolean list1ASC = false;
        boolean list1ASCSec = false;
        boolean list2ASC = false;
        boolean list2ASCSec = false;
        boolean equalReverse = false;

        if (cond0 == JoinCondition.LessThan) {//"<") {
            list1ASC = true;
            list2ASCSec = true;
        } else if (cond0 == JoinCondition.LessThanEqual) {//attSymbols.get(0) == "<=") {
            list1ASC = true;
            list2ASCSec = false;
        } else if (cond0 == JoinCondition.GreaterThan) {//attSymbols.get(0) == ">") {
            list1ASC = false;
            list2ASCSec = false;
        } else if (cond0 == JoinCondition.GreaterThanEqual) {//(attSymbols.get(0) == ">=") {
            list1ASC = false;
            list2ASCSec = true;
        }

        // Reference and secondry pivot sort order
        if (cond1 == JoinCondition.GreaterThan) {//attSymbols.get(1) == ">") {
            list2ASC = true;
            list1ASCSec = true;
        } else if (cond1 == JoinCondition.GreaterThanEqual) {//attSymbols.get(1) == ">=") {
            list2ASC = true;
            list1ASCSec = false;
        } else if (cond1 == JoinCondition.LessThan) {//attSymbols.get(1) == "<") {
            list2ASC = false;
            list1ASCSec = false;
        } else if (cond1 == JoinCondition.LessThanEqual) {//attSymbols.get(1) == "<=") {
            list2ASC = false;
            list1ASCSec = true;
        }

        // For equal pivot and reference
        if (list1ASC != list2ASCSec && list2ASC != list1ASCSec) {
            equalReverse = true;
        }

        return new Tuple5<Boolean, Boolean, Boolean, Boolean, Boolean>(list1ASC, list1ASCSec, list2ASC, list2ASCSec, equalReverse);
    }

    /**
     * Created by khayyzy on 5/19/16.
     */
    public enum JoinCondition {
        GreaterThan, GreaterThanEqual, LessThan, LessThanEqual
    }
}
