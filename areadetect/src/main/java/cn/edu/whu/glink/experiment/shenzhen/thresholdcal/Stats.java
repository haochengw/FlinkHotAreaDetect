package cn.edu.whu.glink.experiment.shenzhen.thresholdcal;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.experiment.shenzhen.Common;
import cn.edu.whu.glink.experiment.shenzhen.areadetect.FileSource;
import cn.edu.whu.glink.experiment.shenzhen.Pipeline;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static cn.edu.whu.glink.experiment.shenzhen.Common.stats;

public class Stats {


  static StreamExecutionEnvironment env = Common.env;

  public static String getStatFile() throws Exception {
    SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw =
            env.addSource(new FileSource(Pipeline.pickDropFilePath))
                    .map(new Common.Parser(Pipeline.RasterSize))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<DetectUnit, Boolean>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                            .withTimestampAssigner((r, timestamp) -> r.f0.getTimestamp()));
    return stats(raw);
  }
}
