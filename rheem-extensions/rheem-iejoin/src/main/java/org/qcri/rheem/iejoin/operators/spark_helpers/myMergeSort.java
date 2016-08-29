package org.qcri.rheem.iejoin.operators.spark_helpers;


import org.qcri.rheem.iejoin.data.Data;

import java.util.Comparator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class myMergeSort<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>> {
    private static final int INSERTIONSORT_THRESHOLD = 7;

    public static void sort(Data[] a, int[] b, Comparator<Data> c) {
        Data[] aux = new Data[a.length];//(Data[]) a.clone();
        System.arraycopy(a, 0, aux, 0, a.length);
        int[] baux = b.clone();
        mergeSort(aux, a, baux, b, 0, a.length, 0, c);
    }

    private static void mergeSort(Data[] src, Data[] dest, int[] bSrc,
                                  int[] bDst, int low, int high, int off, Comparator c) {
        int length = high - low;

        // Insertion sort on smallest arrays
        if (length < INSERTIONSORT_THRESHOLD) {
            for (int i = low; i < high; i++)
                for (int j = i; j > low && c.compare(dest[j - 1], dest[j]) > 0; j--) {
                    swap(dest, j, j - 1);
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
        if (c.compare(src[mid - 1], src[mid]) <= 0) {
            System.arraycopy(src, low, dest, destLow, length);
            System.arraycopy(bSrc, low, bDst, destLow, length);
            return;
        }

        // Merge sorted halves (now in src) into dest
        for (int i = destLow, p = low, q = mid; i < destHigh; i++) {
            if (q >= high || p < mid && c.compare(src[p], src[q]) <= 0) {
                bDst[i] = bSrc[p];
                dest[i] = src[p++];
            } else {
                bDst[i] = bSrc[q];
                dest[i] = src[q++];
            }
        }
    }

    private static void swap(Data[] x, int a, int b) {
        Data t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

    private static void swapp(int[] x, int a, int b) {
        int t = x[a];
        x[a] = x[b];
        x[b] = t;
    }
}
