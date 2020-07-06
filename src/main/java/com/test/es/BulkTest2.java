package com.test.es;

import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BulkTest2 {

    public static void main(String[] args) throws Exception {

        String indexName = args[0];
        String indexType = args[1];
        Integer bulkSize = Integer.valueOf(args[2]);
        Integer maxThreadNum = Integer.valueOf(args[3]);

        String foldList = args[4];

        List<String> list = FileUtils.addFiles(foldList.split(","));
        Collections.shuffle(list);

        LinkedBlockingQueue<String> fileList = new LinkedBlockingQueue<>();
        fileList.addAll(list);

        ExecutorService executorService = Executors.newFixedThreadPool(maxThreadNum);

        int totalThreadNum = 0;

        long startTime = System.currentTimeMillis();
        System.out.println("start time: " + startTime);

        Scanner sc = new Scanner(System.in);

        String cmd = sc.nextLine();
        while (!cmd.equalsIgnoreCase("end")) {
            System.out.println("cmd: " + cmd);
            String[] add = cmd.split(";");

            if (add.length == 2 && add[0].equalsIgnoreCase("add")) {
                int num = Integer.valueOf(add[1]);

                for (int i = 0; i < num; ++i) {
                    executorService.execute(new BulkThread(fileList, indexName, indexType, bulkSize, totalThreadNum++));
                }

                System.out.println("total thread num : " + totalThreadNum);
            }
            cmd = sc.nextLine();
        }

        System.out.println("wait to finish");

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();

        System.out.println("end time: " + endTime);
        System.out.println("spend total time: " + (endTime - startTime) / 1000.0);

    }

}
