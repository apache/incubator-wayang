package org.qcri.rheem.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.qcri.rheem.iejoin.data.Data;
import org.qcri.rheem.iejoin.operators.IEJoinMasterOperator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class BitSetJoin<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
        implements
        PairFlatMapFunction<Tuple2<List2AttributesObjectSkinny<Type0, Type1>, List2AttributesObjectSkinny<Type0, Type1>>, Long, Long> {

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

    public Data[] merge(Data[] lst1, Data[] lst2, boolean asc1, boolean asc2) {
        int totalSize = lst1.length + lst2.length; // every element in the set

        Data[] result = new Data[totalSize];

        int i = 0;
        int j = 0;

        Data.Comparator dc = new Data.Comparator(asc1, asc2);

        // short cut if the two lists where already sorted
        if (dc.compare(lst1[(lst1.length - 1)], lst2[0]) < 0) {
            System.arraycopy(lst1, 0, result, 0, lst1.length);
            System.arraycopy(lst2, 0, result, lst1.length, lst2.length);
            return result;
        }

        int k = 0;
        while (i + j < totalSize) {
            if (i < lst1.length && j < lst2.length) {
                if (dc.compare(lst1[i], lst2[j]) < 0) {
                    result[k++] = (lst1[i++]);
                    // i++;
                } else {
                    result[k++] = (lst2[j++]);
                    // j++;
                }
            } else if (i < lst1.length) {
                // result.add(lst1[i++]);
                // i++;
                System.arraycopy(lst1, i, result, k, (lst1.length - i));
                return result;
            } else {
                // result.add(lst2[j++]);
                // j++;
                System.arraycopy(lst2, j, result, k, (lst2.length - j));
                return result;
            }
        }
        return result;
    }

    public Iterator<Tuple2<Long, Long>> call(
            Tuple2<List2AttributesObjectSkinny<Type0, Type1>, List2AttributesObjectSkinny<Type0, Type1>> arg0)
            throws Exception {
        // ArrayList<Tuple2<Long, Long>> output = new ArrayList<Tuple2<Long,
        // Long>>(1);

        if (sameRDD) {

            Data[] lst1a = arg0._1().getList1();

            int[] permutationArray = new int[lst1a.length];
            for (int i = 0; i < permutationArray.length; i++) {
                permutationArray[i] = i;
            }
            Data[] list2 = new Data[lst1a.length];
            System.arraycopy(lst1a, 0, list2, 0, lst1a.length);

            myMergeSort.sort(list2, permutationArray, new revDataComparator(
                    list2ASC, list2ASCSec, equalReverse));

            ArrayList<Tuple2<Long, Long>> wilResult = getViolationsSelf(lst1a,
                    permutationArray);
            return wilResult.iterator();
        } else {

            Data[] lst1a = arg0._1().getList1();
            Data[] lst1b = arg0._2().getList1();

            // reset pivot flag
            for (int i = 0; i < lst1b.length; i++) {
                lst1b[i].resetPivot();
            }

            Data[] list1 = merge(lst1a, lst1b, list1ASC, list1ASCSec);

            int[] permutationArray = new int[list1.length];
            for (int i = 0; i < permutationArray.length; i++) {
                permutationArray[i] = i;
            }
            Data[] list2 = new Data[list1.length];
            System.arraycopy(list1, 0, list2, 0, list1.length);
            myMergeSort.sort(list2, permutationArray, new revDataComparator(
                    list2ASC, list2ASCSec, equalReverse));

            ArrayList<Tuple2<Long, Long>> wilResult = getViolationsNonSelf(
                    list1, permutationArray);

            return wilResult.iterator();
        }
        // return output;

    }

    private ArrayList<Tuple2<Long, Long>> getViolationsSelf(Data[] cond1,
                                                            int[] permutationArray) {
        ArrayList<Tuple2<Long, Long>> violation = new ArrayList<Tuple2<Long, Long>>(
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
                            violation.add(new Tuple2<Long, Long>(
                                    cond1[permutationArray[k]].getRowID(),
                                    cond1[l].getRowID()));
                        }
                    }
                }
                iter++;
            }

        }
        // System.out.println("CNT = "+cnt);
        return violation;
    }

    private ArrayList<Tuple2<Long, Long>> getViolationsNonSelf(Data[] cond1,
                                                               int[] permutationArray) {
        ArrayList<Tuple2<Long, Long>> violation = new ArrayList<Tuple2<Long, Long>>(
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

            if (cond1[permutationArray[k]].isPivot()) {
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
                                if (!cond1[l].isPivot()) {
                                    // cnt=cnt+1;
                                    violation.add(new Tuple2<Long, Long>(
                                            cond1[permutationArray[k]]
                                                    .getRowID(), cond1[l]
                                            .getRowID()));
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
