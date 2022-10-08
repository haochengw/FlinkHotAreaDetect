`cn/edu/whu/glink/areadetect`包下为用于实现分布式热点区域识别的核心代码, 基本过程依次为:
1. 上下客点->监控单元映射
2. 热点单元识别
3. 并行热点区域识别
4. 热点区域合并

其中热点区域合并部分提供了两种实现方式:
1. 分布式实现(distributed), 借鉴MR-DBSCAN多阶段聚合的实现策略.
2. 集中式实现(centralized), 将所有热点区域汇聚至单节点上, 借助R-tree合并各热点区域几何.

`cn/edu/whu/glink/examples`包下为论文中基于纽约市/武汉市出租车数据集实现分布式聚类的代码, 其中纽约市实验中使用Kafka作为数据源, 
武汉市实验中使用文件作为数据源, 相关的数据文件与预处理代码已省略, 所读取数据记录的格式为: 

```
carID, lng, lat, timestamp, type
```


纽约市数据集为公开数据集, 可至[此处](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)下载.

