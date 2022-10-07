package cn.edu.whu.glink.experiment.shenzhen.areadetect;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.experiment.shenzhen.Common;
import cn.edu.whu.glink.experiment.shenzhen.Pipeline;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static cn.edu.whu.glink.experiment.shenzhen.Common.*;

public class Main {


  public static void headAreaDetect() throws Exception {
    long start = System.currentTimeMillis();

    StreamExecutionEnvironment env = Common.env;
    SingleOutputStreamOperator<Tuple2<DetectUnit, Boolean>> raw =
        env.addSource(new FileSource(Pipeline.pickDropFilePath))
        .map(new Common.Parser(rasterSize))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<DetectUnit, Boolean>>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((r, timestamp) -> r.f0.getTimestamp()  + 28800000));

    detect(raw, start);
  }

}
