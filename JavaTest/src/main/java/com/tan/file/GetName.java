package com.tan.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * 从类似如下的文本文件中读取出所有的姓名，
 * 并打印出重复的姓名和重复的次数，并按重复次数排序
 * Created by Tan on 2017/9/14.
 */
public class GetName {

    public static void main(String[] args) throws Exception{
        String src = "E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\files\\names.txt";
        BufferedReader br = new BufferedReader(new FileReader(new File(src)));
        Map<String,Integer> results = new HashMap<String,Integer>();
        String line = null;
        while ((line=br.readLine())!=null) {
            dealLine(line,results);
        }
        sortAndPrint(results);
    }

    private static void sortAndPrint(Map map) {
        TreeMap<String,Integer> sortMap = new TreeMap<String,Integer>(new Comparator<String>() {
            public int compare(String o1, String o2) {
                return String.valueOf(o1).compareTo(o2);
            }
        });
        sortMap.putAll(map);
//        System.out.println(sortMap);
        Iterator iter = sortMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry m =  (Map.Entry) iter.next();
//            if ((Integer)m.getValue()>1)
                System.out.println(m.getKey()+": "+m.getValue());
        }
    }

    private static void dealLine(String line,Map map) {
        if(!"".equals(line.trim())){
            String[] words = line.split(",");
            if (words.length==3) {
                String name = words[1];
                Integer num = (Integer) map.get(name);
                if (num == null) num = 0;
                map.put(name,num+1);
            }
        }
    }
}
