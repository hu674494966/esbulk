package com.test.es;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FileUtils {
    /**
     * 创建列表，将多个文件夹下文件以及子文件夹文件加入列表中
     *
     * @param folders 文件夹集合
     * @return 文件列表
     */
    public static List<String> addFiles(String[] folders) {
        List<String> fileList = new ArrayList<>();
        for (String folder : folders) {//数据存放路径，可以有多个，中间用逗号分割
            File file = new File(folder);

            if (file.isFile()) {

                fileList.add(file.toString());
            } else {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File f : files) {
                        if (!f.isDirectory()) {
                            //System.out.println(f.toString());//打印文件的绝对路径
                            fileList.add(f.toString());
                        } else {
                            recursion(f.toString(), fileList);
                        }
                    }
                }
            }
        }
        return fileList;
    }

    /**
     * 辅助函数，读取子文件夹中文件
     *
     * @param root     文件夹名
     * @param fileList 文件列表
     */
    private static void recursion(String root,
                                  List<String> fileList) {
        File file = new File(root);
        File[] subFile = file.listFiles();
        if (subFile != null) {
            for (int i = 0; i < subFile.length; i++) {
                if (subFile[i].isDirectory()) {
                    recursion(subFile[i].getAbsolutePath(), fileList);
                } else {
                    fileList.add(subFile[i].getAbsolutePath());
                }
            }
        }
    }

    public static void main(String[] args) {
        String foldList = "/data/hzp_test";
        String[] fd = foldList.split(",");
        List<String> list = addFiles(fd);
        for (int i = 0; i <list.size() ; i++) {
            System.out.println(list.get(i));
        }



    }
}
