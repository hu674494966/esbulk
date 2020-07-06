package com.test.es;

import transwarp.org.elasticsearch.action.search.SearchRequestBuilder;
import transwarp.org.elasticsearch.action.search.SearchResponse;
import transwarp.org.elasticsearch.client.transport.TransportClient;
import transwarp.org.elasticsearch.common.unit.TimeValue;
import transwarp.org.elasticsearch.index.query.QueryBuilders;
import transwarp.org.elasticsearch.search.SearchHit;
import transwarp.org.elasticsearch.search.slice.SliceBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class BulkReadThread implements Runnable {

    private String indexName;
    private String indexType;
    private Integer sliceNum;
    private Integer batchSize;
    private Long fileContentSize;

    private String filePathPre;
    private Integer sliceId;
    private Long maxSearchNum;

    private Integer keepAliveTime = 1;

    public BulkReadThread(String indexName, String indexType, Integer sliceNum, Integer batchSize, Long fileContentSize, Long maxSearchNum, String filePathPre, Integer id) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.sliceNum = sliceNum;
        this.batchSize = batchSize;
        this.fileContentSize = fileContentSize;
        this.maxSearchNum = maxSearchNum;

        this.filePathPre = filePathPre;
        this.sliceId = id;
    }

    @Override
    public void run() {
        System.out.println("thread" + sliceId + " begin");
        bulkRead();
        System.out.println("thread" + sliceId + " over");
    }

    private void bulkRead() {
        TransportClient client = SingleTransportClient.getClient();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSize(batchSize)
                .setScroll(TimeValue.timeValueMinutes(keepAliveTime))
                .setQuery(QueryBuilders.matchAllQuery());

        searchRequestBuilder.slice(new SliceBuilder(sliceId, sliceNum));

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        if (maxSearchNum < 0) {
            maxSearchNum = searchResponse.getHits().getTotalHits();
            System.out.println("slideId: " + sliceId + " has: " + maxSearchNum);
        }

        long originTime = System.currentTimeMillis();
        long startTime = originTime;
        long endTime = 0l;

        int searchTotal = 0;

        try {
            int fileTag = 0;
            int count = 0;

            String filePath = filePathPre + File.separator + sliceId + "_" + fileTag + ".txt";

            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath)));
            do {
                String scrollId = searchResponse.getScrollId();

                for (SearchHit hit : searchResponse.getHits()) {
                    String json = hit.getSourceAsString();
                    if (json != null) {
                        count++;
                        if (count % fileContentSize == 0) {
                            bw.write(json);
                            bw.write("\n");
                            bw.flush();
                            bw.close();

                            endTime = System.currentTimeMillis();
                            fileTag++;
                            count = 0;
                            startTime = endTime;


                            filePath = filePathPre + File.separator + sliceId + "_" + fileTag + ".txt";
                            bw = new BufferedWriter(new FileWriter(new File(filePath)));
                        } else {
                            bw.write(json);
                            bw.write("\n");
                        }

                        searchTotal++;
                        if (searchTotal == maxSearchNum) {
                            System.out.println("break inner reach total: " + searchTotal);
                            break;
                        }
                    }
                }

                if (searchTotal == maxSearchNum) {
                    System.out.println("break outer reach total: " + searchTotal);
                    break;
                }

                searchResponse = client.prepareSearchScroll(scrollId)
                        .setScroll(TimeValue.timeValueMinutes(keepAliveTime)).execute().actionGet();

                if (searchResponse.getHits().getHits().length == 0) {
                    System.out.println("break current total: " + searchTotal);
                    break;
                }
            } while (true);


            bw.flush();
            bw.close();

            endTime = System.currentTimeMillis();
            System.out.println(filePath + " over, spend: " + (endTime - startTime) / 1000.0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        endTime = System.currentTimeMillis();
        System.out.println("sliceId: " + sliceId + " start time: " + originTime);
        System.out.println("sliceId: " + sliceId + " end time: " + endTime);
        System.out.println("sliceId: " + sliceId + " all spend: " + (endTime - originTime) / 1000.0);
        System.out.println("sliceId: " + sliceId + " avg doc/s : " + maxSearchNum * 1000.0 / (endTime - originTime));
    }
}
