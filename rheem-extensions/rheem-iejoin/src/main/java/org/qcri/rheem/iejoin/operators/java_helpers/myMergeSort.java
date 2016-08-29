package org.qcri.rheem.iejoin.operators.java_helpers;

import org.qcri.rheem.iejoin.data.Data;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class myMergeSort<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>, Input> {
    private static final int INSERTIONSORT_THRESHOLD = 7;

    public void sort(ArrayList<Tuple2<Data<Type0, Type1>, Input>> a,
                     int[] b, Comparator<Tuple2<Data<Type0, Type1>, Input>> c) {
        ArrayList<Tuple2<Data<Type0, Type1>, Input>> aux = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>();//new Tuple2<Data<Type0,Type1>,Input>[a.length];//(Data[]) a.clone();
        //System.arraycopy(a, 0, aux, 0, a.size());
        //Collections.copy(a,aux);
        aux.addAll(a);
        int[] baux = b.clone();
        mergeSort(aux, a, baux, b, 0, a.size(), 0, c);
    }

    private void mergeSort(ArrayList<Tuple2<Data<Type0, Type1>, Input>> src,
                           ArrayList<Tuple2<Data<Type0, Type1>, Input>> dest, int[] bSrc,
                           int[] bDst, int low, int high, int off, Comparator c) {
        int length = high - low;

        // Insertion sort on smallest arrays
        if (length < INSERTIONSORT_THRESHOLD) {
            for (int i = low; i < high; i++)
                for (int j = i; j > low && c.compare((Tuple2<Data<Type0, Type1>, Input>) dest.get(j - 1), (Tuple2<Data<Type0, Type1>, Input>) dest.get(j)) > 0; j--) {
                    //swap(dest, j, j - 1);
                    java.util.Collections.swap(dest, j, j - 1);
                    swapp(bDst, j, j - 1);
                }
            return;
        }

        // Recursively sort halves of dest into src
        int destLow = low;
        int destHigh = high;
        low += off;
        high += off;
        int mid = (low + high) >>> 1;
        mergeSort(dest, src, bDst, bSrc, low, mid, -off, c);
        mergeSort(dest, src, bDst, bSrc, mid, high, -off, c);

        // If list is already sorted, just copy from src to dest. This is an
        // optimization that results in faster sorts for nearly ordered lists.
        if (c.compare(src.get(mid - 1)._1(), src.get(mid)._1()) <= 0) {
            //System.arraycopy(src, low, dest, destLow, length);
            int jjj = destLow;
            for (int iii = low; iii < length; iii++) {
                dest.set(jjj, src.get(iii));
            }
            System.arraycopy(bSrc, low, bDst, destLow, length);
            return;
        }

        // Merge sorted halves (now in src) into dest
        for (int i = destLow, p = low, q = mid; i < destHigh; i++) {
            if (q >= high || p < mid && c.compare((Tuple2<Data<Type0, Type1>, Input>) src.get(p), (Tuple2<Data<Type0, Type1>, Input>) src.get(q)) <= 0) {
                bDst[i] = bSrc[p];
                dest.set(i, src.get(p++));
            } else {
                bDst[i] = bSrc[q];
                dest.set(i, src.get(q++));
            }
        }
    }

    private void swap(ArrayList<Tuple2<Data<Type0, Type1>, Input>> x, int a, int b) {
        java.util.Collections.swap(x, a, b);
    }

    private void swapp(int[] x, int a, int b) {
        int t = x[a];
        x[a] = x[b];
        x[b] = t;
    }
}
