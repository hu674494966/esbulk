package com.test.es;

import transwarp.org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import transwarp.org.elasticsearch.client.transport.TransportClient;

public class ESApiDemo {

    public void createIndex() {
        // 1 创建索引
        System.out.println("开始创建！！！");
        TransportClient client = SingleTransportClient.getClient();
        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate("blog").get();
        if (indexResponse.isAcknowledged()) {// true表示创建成功
            System.out.println("es表创建成功！！！");
        }

        // 2 关闭连接
        client.close();
    }


    public static void main(String[] args) {
        ESApiDemo esApiDemo = new ESApiDemo();
       // esApiDemo.createIndex();
        long endTime = System.currentTimeMillis();
        System.out.println(endTime);

    }
}
