package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * 仅对被识别为异常的检测单元执行冗余路由。
 */
public class RedundantRouter implements FlatMapFunction<DetectUnit, Tuple2<Long, DetectUnit>> {

  @Override
  public void flatMap(DetectUnit unit, Collector<Tuple2<Long, DetectUnit>> collector) throws Exception {
    Set<Long> nearByPartitions = unit.getNearByPartitions();
    // 发向自己的分区
    long mainPartition = unit.getMainPartition();
    collector.collect(new Tuple2<>(mainPartition, unit));
    //  发向周边分区
    for (long nearByPartition : nearByPartitions) {
        collector.collect(new Tuple2<>(nearByPartition, unit));
    }
  }
}
