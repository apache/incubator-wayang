package org.qcri.rheem.iejoin.operators.java_helpers;

import org.qcri.rheem.iejoin.data.Data;
import org.qcri.rheem.iejoin.operators.IEJoinMasterOperator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.BitSet;

/**
 * Created by khayyzy on 5/28/16.
 */
public class BitSetJoin<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>, Input> {

    /**
     *
     */
    private static final long serialVersionUID = 2953731260972596253L;

    boolean list1ASC;
    boolean list2ASC;
    boolean list1ASCSec;
    boolean list2ASCSec;
    boolean equalReverse;
    boolean sameRDD;
    // boolean trimEqResults;
    IEJoinMasterOperator.JoinCondition c1;

    public BitSetJoin(boolean list1ASC, boolean list2ASC, boolean list1ASCSec,
                      boolean list2ASCSec, boolean equalReverse, boolean sameRDD,
                      IEJoinMasterOperator.JoinCondition c1) {
        this.list1ASC = list1ASC;
        this.list2ASC = list2ASC;
        this.list1ASCSec = list1ASCSec;
        this.list2ASCSec = list2ASCSec;
        this.equalReverse = equalReverse;
        this.sameRDD = sameRDD;
        this.c1 = c1;
    }

    public ArrayList<Tuple2<Data<Type0, Type1>, Input>> merge(ArrayList<Tuple2<Data<Type0, Type1>, Input>> lst1,
                                                              ArrayList<Tuple2<Data<Type0, Type1>, Input>> lst2,
                                                              boolean asc1, boolean asc2) {
        int totalSize = lst1.size() + lst2.size(); // every element in the set

        ArrayList<Tuple2<Data<Type0, Type1>, Input>> result = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>(totalSize);

        int i = 0;
        int j = 0;

        DataComparator<Type0, Type1, Input> dc = new DataComparator<>(asc1, asc2);

        // short cut if the two lists where already sorted
        if (dc.compare(lst1.get(lst1.size() - 1), lst2.get(0)) < 0) {
            //System.arraycopy(lst1, 0, result, 0, lst1.size());
            int jj = 0;
            for (int ii = 0; ii < lst1.size(); ii++) {
                result.add(lst1.get(ii));
            }
            int jjj = lst1.size();
            for (int iii = 0; iii < lst2.size(); iii++) {
                result.add(lst2.get(iii));
            }
            //System.arraycopy(lst2, 0, result, lst1.size(), lst2.size());
            return result;
        }

        int k = 0;
        while (i + j < totalSize) {
            if (i < lst1.size() && j < lst2.size()) {
                if (dc.compare(lst1.get(i), lst2.get(j)) < 0) {
                    result.add(lst1.get(i++));
                    // i++;
                } else {
                    result.add((lst2.get(j++)));
                    // j++;
                }
            } else if (i < lst1.size()) {
                // result.add(lst1[i++]);
                // i++;
                //System.arraycopy(lst1, i, result, k, (lst1.size() - i));
                int jj = k;
                for (int x = i; x < (lst1.size() - i); x++) {
                    result.add(lst1.get(x));
                    jj++;
                }
                return result;
            } else {
                // result.add(lst2[j++]);
                // j++;
                //System.arraycopy(lst2, j, result, k, (lst2.size() - j));
                int jjj = k;
                for (int iii = j; iii < (lst2.size() - j); iii++) {
                    result.add(lst2.get(iii));
                }
                return result;
            }
        }
        return result;
    }

    public ArrayList<Tuple2<Input, Input>> call(ArrayList<Tuple2<Data<Type0, Type1>, Input>> lst1a,
                                                ArrayList<Tuple2<Data<Type0, Type1>, Input>> lst1b) {
        // ArrayList<Tuple2<Long, Long>> output = new ArrayList<Tuple2<Long,
        // Long>>(1);

        if (sameRDD) {

            int[] permutationArray = new int[lst1a.size()];
            for (int i = 0; i < permutationArray.length; i++) {
                permutationArray[i] = i;
            }
            ArrayList<Tuple2<Data<Type0, Type1>, Input>> list2 = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>();//new Tuple2<Data<Type0,Type1>,Input>[lst1a.length];
            //Collections.copy(lst1a,list2);
            list2.addAll(lst1a);

            new myMergeSort<Type0, Type1, Input>().sort(list2, permutationArray, new revDataComparator<Type0, Type1, Input>(
                    list2ASC, list2ASCSec, equalReverse));

            ArrayList<Tuple2<Input, Input>> wilResult = getViolationsSelf(lst1a,
                    permutationArray);
            return wilResult;
        } else {

            // reset pivot flag
            for (int i = 0; i < lst1b.size(); i++) {
                lst1b.get(i)._1().resetPivot();
            }

            ArrayList<Tuple2<Data<Type0, Type1>, Input>> list1 = merge(lst1a, lst1b, list1ASC, list1ASCSec);

            int[] permutationArray = new int[list1.size()];
            for (int i = 0; i < permutationArray.length; i++) {
                permutationArray[i] = i;
            }
            ArrayList<Tuple2<Data<Type0, Type1>, Input>> list2 = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>();//Tuple2<Data<Type0,Type1>,Record>[list1.length];
            //System.arraycopy(list1, 0, list2, 0, list1.size());
            //Collections.copy(list1,list2);
            list2.addAll(list1);
            new myMergeSort<Type0, Type1, Input>().sort(list2, permutationArray, new revDataComparator<Type0, Type1, Input>(
                    list2ASC, list2ASCSec, equalReverse));

            ArrayList<Tuple2<Input, Input>> wilResult = getViolationsNonSelf(
                    list1, permutationArray);

            return wilResult;
        }
        // return output;

    }

    private ArrayList<Tuple2<Input, Input>> getViolationsSelf(ArrayList<Tuple2<Data<Type0, Type1>, Input>> cond1,
                                                              int[] permutationArray) {
        ArrayList<Tuple2<Input, Input>> violation = new ArrayList<Tuple2<Input, Input>>(
                300000);
        long cnt = 0;
        int chunckSize = Math.min(permutationArray.length, 1024); // in bit

        BitSet bitArray = new BitSet(permutationArray.length);
        int indexSize = permutationArray.length / chunckSize;

        if (permutationArray.length % chunckSize != 0)
            ++indexSize;

        short[] bitIndex = new short[indexSize];

        for (int k = 0; k < bitIndex.length; k++) {
            bitIndex[k] = 0;
        }

        int max = 0;
        int offset = (equalReverse == true ? 0 : 1);

        for (int k = 0; k < permutationArray.length; k++) {

            // scan bit index

            int bIndex = permutationArray[k] / chunckSize;
            int iter = 0;
            // if both conditions are equal do a self join

            bitArray.set(permutationArray[k]);
            bitIndex[bIndex] = (short) (bitIndex[bIndex] + 1);
            max = Math.max(max, bIndex + 1);

            for (int z = bIndex; z < max; z++) {

                if (bitIndex[z] > 0) {
                    // scan the chunk
                    int start = iter == 0 ? permutationArray[k] + offset : z
                            * chunckSize;
                    int end = Math.min((z * chunckSize) + chunckSize,
                            permutationArray.length);

                    for (int l = start; l < end; l++) {
                        if (bitArray.get(l)) {
                            violation.add(new Tuple2<Input, Input>(
                                    cond1.get(permutationArray[k])._2(),//getRowID(),
                                    cond1.get(l)._2()));//getRowID()));
                        }
                    }
                }
                iter++;
            }
        }
        // System.out.println("CNT = "+cnt);
        return violation;
    }

    private ArrayList<Tuple2<Input, Input>> getViolationsNonSelf(ArrayList<Tuple2<Data<Type0, Type1>, Input>> cond1,
                                                                 int[] permutationArray) {
        ArrayList<Tuple2<Input, Input>> violation = new ArrayList<Tuple2<Input, Input>>(
                300000);
        long cnt = 0;
        int chunckSize = Math.min(permutationArray.length, 1024); // in bit

        BitSet bitArray = new BitSet(permutationArray.length);
        int indexSize = permutationArray.length / chunckSize;

        if (permutationArray.length % chunckSize != 0)
            ++indexSize;

        short[] bitIndex = new short[indexSize];

        for (int k = 0; k < bitIndex.length; k++) {
            bitIndex[k] = 0;
        }

        int max = 0;
        for (int k = 0; k < permutationArray.length; k++) {

            // scan bit index if only cond1.get(permutationArray[k]) is primary
            // pivot

            int bIndex = permutationArray[k] / chunckSize;

            bitArray.set(permutationArray[k]);
            bitIndex[bIndex] = (short) (bitIndex[bIndex] + 1);
            max = Math.max(max, bIndex + 1);

            if (cond1.get(permutationArray[k])._1().isPivot()) {
                int iter = 0;
                for (int z = bIndex; z < max; z++) {

                    if (bitIndex[z] > 0) {
                        // scan the chunk
                        int start = iter == 0 ? permutationArray[k] + 1 : z
                                * chunckSize;
                        int end = Math.min((z * chunckSize) + chunckSize,
                                permutationArray.length);

                        for (int l = start; l < end; l++) {
                            if (bitArray.get(l)) {
                                if (!cond1.get(l)._1().isPivot()) {
                                    // cnt=cnt+1;
                                    violation.add(new Tuple2<Input, Input>(
                                            cond1.get(permutationArray[k])._2()
                                            //.getRowID()
                                            , cond1.get(l)._2()));//.getRowID()));
                                }
                            }
                        }
                    }
                    iter++;
                }
            }
        }
        // System.out.println("CNT = "+cnt);
        return violation;
    }
}
