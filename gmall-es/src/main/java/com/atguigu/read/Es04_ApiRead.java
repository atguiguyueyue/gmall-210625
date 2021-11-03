package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Es04_ApiRead {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.读数据
        //TODO -----------------------------{}--------------------------------
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //TODO -----------------------------bool------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //TODO -----------------------------term------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");
        //TODO -----------------------------filter------------------------------
        boolQueryBuilder.filter(termQueryBuilder);
        //TODO -----------------------------query------------------------------
        searchSourceBuilder.query(boolQueryBuilder);
        //TODO -----------------------------match------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "乒乓球");
        //TODO -----------------------------must------------------------------
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO -----------------------------terms------------------------------
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByclass").field("class_id");

        //TODO -----------------------------max------------------------------
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");

        //TODO -----------------------------aggs------------------------------
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));

        //TODO -----------------------------from------------------------------
        searchSourceBuilder.from(0);

        //TODO -----------------------------size------------------------------
        searchSourceBuilder.size(2);


        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);

        //a.获取命中数据条数
        System.out.println("total:" + result.getTotal());

        //b.获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            Map map = hit.source;
            for (Object o : map.keySet()) {
                System.out.println(o + ":" + map.get(o));
            }
        }

        //c.获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        //c.2班级聚合组数据
        TermsAggregation groupByclass = aggregations.getTermsAggregation("groupByclass");
        List<TermsAggregation.Entry> buckets = groupByclass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());

            //c.3获取年龄聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:" + groupByAge.getMax());
        }

        //关闭连接
        jestClient.shutdownClient();
    }
}
