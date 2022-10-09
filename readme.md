## 代码结构
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
time,lng,lat,type
```
纽约市数据集为公开数据集, 可至[此处](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)下载.

`cn/edu/whu/glink/examples/threshold`包中实现了单峰阈值选取算法, 基于给定的监控单元大小与时间窗口, 为数据集计算推荐的热点阈值.

## 运行

### 1. Maven打包
```
mvn clean package
```

### 2. 提交作业

```
cd areadetect/target
flink run -p 1 -c cn.edu.whu.glink.examples.nyc.Job ./HotAreaDetect-0.1-SNAPSHOT-jar-with-dependencies.jar 0.1 10 10 7 10 nyc-2015-JAN cent
```

其中, `-p`指定了作业的并行度, 末尾的7个参数依次为:
0.1 10 10 7 10 nyc-2015-JAN cent
- 监控单元大小s
- 热点识别滑动窗口长度
- 热点识别滑动窗口步长
- 热点阈值v
- 分区大小pSize
- 作业消费的Kafka Topic
- 算法类型, `dist`为分布式算法, 为论文中所提出的方法, `cent`为集中式算法, 为论文中baseline.

### 3. 成果展示

热点区域以Geometry对象的形式输出, `userData`中含有该热点区域的ID, 时间, 面积, 热点单元平均上下客密度, 可使用`cn.edu.whu.glink.examples.shp.ShapeFileUtil`转为shp文件实现可视化.

