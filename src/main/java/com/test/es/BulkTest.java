package com.test.es;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BulkTest {


    public static void main(String[] args) throws Exception {

        String indexName = args[0];
        String indexType = args[1];
        Integer bulkSize = Integer.valueOf(args[2]);
        Integer threadNum = Integer.valueOf(args[3]);

        String foldList = args[4];

        List<String> list = FileUtils.addFiles(foldList.split(","));
        Collections.shuffle(list);
        LinkedBlockingQueue<String> fileList = new LinkedBlockingQueue<>();

       /* LinkedBlockingQueue<String> fileList = new LinkedBlockingQueue<>();
        fileList.addAll(list);*/

        long startTime = System.currentTimeMillis();
        System.out.println("start time: " + startTime);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        for (int i = 0; i < threadNum; ++i) {
            fileList.addAll(list);
            executorService.execute(new BulkThread(fileList, indexName, indexType, bulkSize, i));
        }

        System.out.println("wait to finish");

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();

        System.out.println("end time: " + endTime);
        System.out.println("spend total time: " + (endTime - startTime) / 1000.0);

    }

}
