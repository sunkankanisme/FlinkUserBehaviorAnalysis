## 项目模块说明

### HotItemAnalysis

> 热门商品分析
> - `com.sunk.topn.HotItems`: 实时 topN 统计
> - `com.sunk.topn.HotItemsWithSql`: 实时 topN 统计, 使用 TableApi
> - `com.sunk.topn.KafkaProducerUtil`: Kafka 测试数据生产者

### NetworkFlowAnalysis

> 网络流量指标分析
> - `com.sunk.flow.HotPagesNetworkFlow`: 实时 TOP Url 访问量统计
> - `com.sunk.flow.PageView`: 实时 pv 统计
> - `com.sunk.flow.UniqueVisitor`: 实时 uv 统计
> - `com.sunk.flow.UvWithBloom`: 实时 uv 统计，使用布隆过滤器计数

### MarketAnalysis

> 市场营销指标分析
> - `com.sunk.market.AppMarketByChannel`: 分渠道统计指标
> - `com.sunk.market.AdClickAnalysis`: 页面广告分析

