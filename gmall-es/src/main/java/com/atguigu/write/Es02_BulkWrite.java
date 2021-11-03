package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据
        Movie movie103 = new Movie("103", "你的名字");
        Movie movie104 = new Movie("104", "我的名字");
        Movie movie105 = new Movie("105", "他的名字");
        Movie movie106 = new Movie("106", "谁的名字");

        Index index103 = new Index.Builder(movie103).id("1003").build();
        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();
        Index index106 = new Index.Builder(movie106).id("1006").build();

        Bulk build = new Bulk.Builder()
                .defaultType("_doc")
                .defaultIndex("movie_0625")
                .addAction(index103)
                .addAction(index104)
                .addAction(index105)
                .addAction(index106)
                .build();
        jestClient.execute(build);

        //关闭连接
        jestClient.shutdownClient();
    }
}
