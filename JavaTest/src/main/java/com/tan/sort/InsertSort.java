package com.tan.sort;

import java.util.Arrays;

/**
 * Created by Tan on 2017/8/3.
 */
public class InsertSort {
    public static void main(String[] args) {
        int[] arr = {32,17,43,66,25,39,12};
        sort(arr,7);
        System.out.println(Arrays.toString(arr));
    }

    private static void sort(int[] arr, int right){
        for (int i=1; i < right; i++) {
            int x = arr[i];
            int j;
            for (j=i-1; j>=0; j--) {
                if (x<arr[j]) {
                    arr[j + 1] = arr[j];
                } else break;
            }
            arr[j+1] = x;
        }
    }
}
