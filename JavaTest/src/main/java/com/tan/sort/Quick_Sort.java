package com.tan.sort;

import java.util.Arrays;

/**
 * Created by Tan on 2017/8/2.
 */
public class Quick_Sort {
    public static void main(String[] args) {
        int[] arr = {23,13,53,35,21,5,30};
        quick_sort(arr,0,arr.length-1);
        System.out.println(Arrays.toString(arr));
    }
    //   x=23     5,13,53,35,21,5,30
    //快速排序
    private static void quick_sort(int s[], int l, int r)
    {
        if (l < r)
        {
            //Swap(s[l], s[(l + r) / 2]); //将中间的这个数和第一个数交换 参见注1
            int i = l, j = r, x = s[l];
            while (i < j)
            {
                while(i < j && s[j] >= x) // 从右向左找第一个小于x的数
                    j--;
                if(i < j)
                    s[i++] = s[j];
                    System.out.println("i:"+i + "  j:"+j +"  "+Arrays.toString(s));

                while(i < j && s[i] < x) // 从左向右找第一个大于等于x的数
                    i++;
                if(i < j)
                    s[j--] = s[i];
                    System.out.println("i:"+i + "  j:"+j +"  "+Arrays.toString(s));
            }
            s[i] = x;
            quick_sort(s, l, i - 1); // 递归调用
            quick_sort(s, i + 1, r);
        }
    }
}

