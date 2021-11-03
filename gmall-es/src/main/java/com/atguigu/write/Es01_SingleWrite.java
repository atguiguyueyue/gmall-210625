package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es01_SingleWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();


        //4.写入单条数据
        Movie movie = new Movie("102", "请回答1988");

        Index index = new Index.Builder(movie)
                .id("1002")
                .type("_doc")
                .index("movie_0625")
                .build();


//        Index index = new Index.Builder("{\n" +
//                "  \"id\":101,\n" +
//                "  \"movie_name\":\"奥特曼大战葫芦娃\"\n" +
//                "}")
//                .index("movie_0625")
//                .type("_doc")
//                .id("1001")
//                .build();

        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
