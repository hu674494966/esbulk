package com.test.es;

import transwarp.org.elasticsearch.client.transport.TransportClient;
import transwarp.org.elasticsearch.common.settings.Settings;
import transwarp.org.elasticsearch.common.transport.InetSocketTransportAddress;
import transwarp.org.elasticsearch.common.transport.TransportAddress;
import transwarp.org.elasticsearch.transport.client.PreBuiltTransportClient;


import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class SingleTransportClient {
    private static TransportClient client;

    private SingleTransportClient() {
    }

    public static TransportClient getClient() {
        if (client == null) {

            synchronized(SingleTransportClient.class) {
                if (client == null) {
                    //Settings settings = Settings.builder().put("cluster.name", "es6.4.3").build();
                    Settings settings = Settings.builder().put("cluster.name", "mycluster").build();
                    System.out.println("init client");

                    try {
                        client = new PreBuiltTransportClient(settings, new Class[0]);
                        //String[] hosts = {"192.168.1.5", "192.168.1.6", "192.168.1.7","192.168.1.8"};
                        String[] hosts = {"192.168.1.6", "192.168.1.7","192.168.1.8"};
                        for (String host : hosts) {
                            //client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), 9300));
                            client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, 9300)));
                        }

                        //client.addTransportAddress(new TransportAddress(InetAddress.getByName("172.21.0.5"), 9300)).addTransportAddress(new TransportAddress(InetAddress.getByName("172.21.0.12"), 9300));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    System.out.println("init client over");
                }
            }
        }

        return client;
    }
}
