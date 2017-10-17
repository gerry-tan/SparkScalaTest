package com.tan.file;

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;

/**
 *
 * Created by Tan on 2017/9/14.
 */
public class FileTest {

    /**
     * 1.编写一个程序，将a.txt文件中的单词与b.txt文件中的单词交替合并到c.txt文件中，
     * a.txt文件中的单词用回车符分隔，b.txt文件中用回车或空格进行分隔
     */
    @Test
    public void fileDeal() throws Exception{
        String file_a = "E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\files\\a.txt";
        String file_b = "E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\files\\b.txt";
        String file_c = "E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\files\\c.txt";
        File file = new File(file_c);
        if(!file.exists()){
            file.createNewFile();
        }
        BufferedReader a = new BufferedReader(new FileReader(file_a));
        BufferedReader b = new BufferedReader(new FileReader(file_b));
        FileOutputStream out = new FileOutputStream(file,false);//写入方式：覆盖写
        String line_a,line_b;
        ArrayList arr_a = new ArrayList();
        ArrayList arr_b = new ArrayList();
        while((line_a = a.readLine())!=null) {
            for(String sa:line_a.split("\\s")){
                arr_a.add(sa);
            }
        }
        while((line_b = b.readLine())!=null) {
            for(String sb:line_b.split("\\s")){
                arr_b.add(sb);
            }
        }
        int length = arr_a.size()>arr_b.size()?arr_a.size():arr_b.size();
        for (int i=0;i<length;i++) {
            if (i<arr_a.size())
                out.write((arr_a.get(i).toString()+"\n").getBytes());
            if (i<arr_b.size())
                out.write((arr_b.get(i).toString()+"\n").getBytes());
        }
        a.close();
        b.close();
        out.close();
    }

    @Test
    public void fileClone() throws Exception{
        File dir = new File("E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\files");
        if (!dir.exists() && dir.isDirectory()) {
            System.out.println("目录不存在");
        }
        File[] files = dir.listFiles(new FilenameFilter(){
            public boolean accept(File dir, String name) {
                return name.endsWith(".txt");
            }
        });
        File destDir = new File("E:\\soft\\hadoop_wks\\SparkScalaTest\\JavaTest\\datas");
        if (!destDir.exists() && destDir.isDirectory()) {
            destDir.mkdir();
        }
        for (File f: files) {
            FileInputStream fis = new FileInputStream(f);
            String destFileName = f.getName().replaceAll("\\.txt$",".data");
            FileOutputStream fos = new FileOutputStream(new File(destDir,destFileName));
            int dat;
            while ((dat = fis.read())!=-1) {
                fos.write(fis.read());
            }
            fis.close();
            fos.close();
        }
    }
}
