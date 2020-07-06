package com.test.es;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BulkToFile2 {

    public static void main(String[] args) throws Exception {
        if (args.length < 8) {
            System.out.println("indexName indexType sliceNum batchSize fileContentSize maxSearchNum filePath maxThreadNum");
            System.exit(-1);
        }

        String indexName = args[0];
        String indexType = args[1];

        Integer sliceNum = Integer.valueOf(args[2]);
        Integer batchSize = Integer.valueOf(args[3]);

        Long fileContentSize = Long.valueOf(args[4]);
        Long maxSearchNum = Long.valueOf(args[5]);

        String filePathPre = args[6];

        Integer maxThreadNum = Integer.valueOf(args[7]);

        if (maxThreadNum > sliceNum) {
            System.out.println("maxThreadNum must less than sliceNum");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        System.out.println("process start time: " + startTime);

        ExecutorService executorService = Executors.newFixedThreadPool(maxThreadNum);

        Scanner sc = new Scanner(System.in);

        String cmd = sc.nextLine();

        int totalThreadNum = 0;

        while (!cmd.equalsIgnoreCase("end")) {
            System.out.println("cmd: " + cmd);
            String[] add = cmd.split(";");

            if (add.length == 2 && add[0].equalsIgnoreCase("add")) {
                int num = Integer.valueOf(add[1]);

                for (int i = 0; i < num; ++i) {
                    executorService.execute(
                            new BulkReadThread(indexName, indexType,
                                    sliceNum, batchSize, fileContentSize, maxSearchNum, filePathPre, totalThreadNum++));
                }

                System.out.println("total thread num : " + totalThreadNum);
            }
            cmd = sc.nextLine();
        }

        System.out.println("process wait to finish");

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();

        System.out.println("process end time: " + endTime);
        System.out.println("spend total time: " + (endTime - startTime) / 1000.0);
        System.out.println("total avg doc/s: " + maxSearchNum * totalThreadNum * 1000.0 / (endTime - startTime));

    }
}
