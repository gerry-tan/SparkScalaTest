package com.tan.sort;

import java.util.Arrays;

/**
 * Created by Tan on 2017/8/3.
 */
public class SelectSort {
    public static void main(String[] args) {
        int[] arr = {32,17,43,66,25,39,21};
        selectSort(arr,7);
        System.out.println(Arrays.toString(arr));
    }

    /**
     * 选择排序：外循环，取当前下标值为参考设为temp；内循环，每一次与temp比较小的则在数组中互换位置并重置temp值
     * @param arr:数组
     * @param n：数组长度
     */
    private static void selectSort(int[] arr, int n) {
        for (int i=0; i<n; i++) {
            int temp = arr[i];
            int j;
            for (j=i+1; j<n; j++){
                if (arr[j] < temp){
                    arr[i] = arr[j];
                    arr[j] = temp;
                    temp = arr[i];
                }
            }
        }
    }
}
