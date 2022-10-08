package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

/**
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class HotDetectUnit {
  public static SingleOutputStreamOperator<DetectUnit> getHotGridStream(DataStream<DetectUnit> input,
                                                                        SlidingEventTimeWindows slidingWindow,
                                                                        double thres,
                                                                        double rasterSize,
                                                                        double partitionSize,
                                                                        double distThreshold) {
    return input
        .keyBy(DetectUnit::getId)
        .window(slidingWindow)
        .aggregate(new SumCountAggregator())
        .filter(r -> (Integer) r.getVal() >= thres)
        .map(new UniGridInfoJoiner(rasterSize, partitionSize, distThreshold));
  }

  public static class SumCountAggregator implements AggregateFunction<DetectUnit, DetectUnit, DetectUnit> {

    @Override
    public DetectUnit createAccumulator() {
      return null;
    }

    @Override
    public DetectUnit add(DetectUnit value, DetectUnit accumulator) {
      if(accumulator == null) {
        accumulator = new DetectUnit(value.getId(), value.getTimestamp(), value.getVal());
      } else {
        accumulator.setVal((Integer) accumulator.getVal() + 1);
      }
      return accumulator;
    }

    @Override
    public DetectUnit getResult(DetectUnit accumulator) {
      return accumulator;
    }

    @Override
    public DetectUnit merge(DetectUnit a, DetectUnit b) {
      a.setVal((Integer) a.getVal() + (Integer) b.getVal());
      return a;
    }
  }

}
