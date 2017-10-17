package com.tan.sort;

import java.util.Arrays;

/**
 * Created by Tan on 2017/8/3.
 */
public class MergeSort {

    private static void merge(int[] arr, int left, int mid, int right) {
        int[] s = new int[right-left+1];
        int i=left,m=mid+1;
        int k=0;
        while(i<=mid && m<=right) {
            if (arr[i] < arr[m]) {
                s[k++] = arr[i++];
            }else {
                s[k++] = arr[m++];
            }
        }
        while (i<=mid) {
            s[k++] = arr[i++];
        }
        while (m<=right) {
            s[k++] = arr[m++];
        }
        for (int n=0; n<s.length; n++) {
            arr[n+left] = s[n];
        }
    }

    private static void sort(int[] arr, int left, int right) {
        int middle = (left + right) / 2;
        if (left<right) {
            sort(arr,left,middle);
            sort(arr,middle+1,right);
            merge(arr,left,middle,right);
        }
    }

    public static void main(String[] args) {
        int[] arr = {32,17,43,66,25,39,21,58};
        sort(arr,0,arr.length-1);
        System.out.println(Arrays.toString(arr));
    }
}
