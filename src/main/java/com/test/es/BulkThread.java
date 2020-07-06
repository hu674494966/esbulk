package com.test.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import transwarp.org.elasticsearch.action.bulk.BulkRequestBuilder;
import transwarp.org.elasticsearch.action.bulk.BulkResponse;
import transwarp.org.elasticsearch.action.index.IndexRequest;
import transwarp.org.elasticsearch.client.transport.TransportClient;
import transwarp.org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class BulkThread implements Runnable {

    private LinkedBlockingQueue<String> fileList;
    private String indexName;
    private String indexType;
    private Integer bulkSize;
    private Integer threadID;

    public BulkThread(LinkedBlockingQueue<String> fileList, String indexName, String indexType, Integer bulkSize, int threadID) {
        this.fileList = fileList;
        this.indexName = indexName;
        this.indexType = indexType;
        this.bulkSize = bulkSize;
        this.threadID = threadID;
    }

    @Override
    public void run() {
        System.out.println("thread" + threadID + " begin");
        bulkLoad();
        System.out.println("thread" + threadID + " over");
    }

    private void bulkLoad() {
    /*    while (fileList.size() != 0) {
            String file = fileList.poll();
            if (file != null) {
                insertByFile(file);
            }
        }*/
        System.out.println("start file count: " + fileList.size() );
        String file = fileList.poll();
        System.out.println("end file count: " + fileList.size());
        insertByFile(file);
    }

    private void insertByFile(String file) {
        String data = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));


            TransportClient client = SingleTransportClient.getClient();
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            int count = 0;
            ObjectMapper mapper = new ObjectMapper();

            while ((data = br.readLine()) != null) {

                Map<String, Object> jsonMap = null;
                try {//Read Map from JSON String,[eg:String jsonObject = "{\"brand\":\"ford\", \"doors\":5}";]
                    jsonMap = mapper.readValue(data, new TypeReference<Map<String, Object>>() {
                    });
                } catch (IOException e) {
                    System.out.println("error data: " + data);
                    e.printStackTrace();
                }


                IndexRequest indexRequest = new IndexRequest(
                        indexName,
                        indexType);


                indexRequest.source(jsonMap, XContentType.JSON);
                bulkRequestBuilder.add(indexRequest);

                count++;

                if (count % bulkSize == 0) {
                    BulkResponse bulkResponse = bulkRequestBuilder.get();
                    if (bulkResponse.hasFailures()) {
                        System.out.println("bulk error");
                        System.out.println(bulkResponse.iterator().next().getFailureMessage());
                    }
                    //重新获取一次bulkRequest,否则会不断的提交之前索引过的数据，最后导致提交的数据量滚雪球式增长，耗尽内存
                    bulkRequestBuilder = client.prepareBulk();
                }
            }

            if (count != 0) {
                BulkResponse bulkResponse = bulkRequestBuilder.get();
                if (bulkResponse.hasFailures()) {
                    System.out.println("bulk error");
                    System.out.println(bulkResponse.iterator().next().getFailureMessage());
                }
            }

            br.close();
        } catch (Exception e) {
            System.out.println("out error : " + data);
            e.printStackTrace();
        }
    }
}