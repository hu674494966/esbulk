package com.test.es;

import transwarp.org.elasticsearch.action.search.SearchRequestBuilder;
import transwarp.org.elasticsearch.action.search.SearchResponse;
import transwarp.org.elasticsearch.client.transport.TransportClient;
import transwarp.org.elasticsearch.index.query.QueryBuilders;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BulkToFile {


    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.out.println("indexName indexType batchSize fileContentSize maxSearchNum filePath threadNum");
            System.exit(-1);
        }

        String indexName = args[0];
        String indexType = args[1];


        Integer batchSize = Integer.valueOf(args[2]);

        Long fileContentSize = Long.valueOf(args[3]);
        Long maxSearchNum = Long.valueOf(args[4]);

        String filePathPre = args[5];

        Integer threadNum = Integer.valueOf(args[6]);

        Integer sliceNum = threadNum;

        if (threadNum > sliceNum) {
            System.out.println("threadNum must less than sliceNum");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        System.out.println("process start time: " + startTime);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        for (int i = 0; i < threadNum; ++i) {
            executorService.execute(
                    new BulkReadThread(indexName, indexType,
                            sliceNum, batchSize, fileContentSize, maxSearchNum, filePathPre, i));
        }

        System.out.println("process wait to finish");

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        long endTime = System.currentTimeMillis();

        System.out.println("process end time: " + endTime);
        System.out.println("spend total time: " + (endTime - startTime) / 1000.0);

        TransportClient client = SingleTransportClient.getClient();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSize(0)
                .setQuery(QueryBuilders.matchAllQuery())
                ;

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        Long totalDoc = searchResponse.getHits().getTotalHits();

        System.out.println("total document: " + totalDoc);

        System.out.println("total avg doc/s: " + totalDoc * 1000.0 / (endTime - startTime));

    }
}
