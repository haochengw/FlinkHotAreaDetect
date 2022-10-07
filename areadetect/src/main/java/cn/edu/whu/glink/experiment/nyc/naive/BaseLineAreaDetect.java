package cn.edu.whu.glink.experiment.nyc.naive;

import cn.edu.whu.glink.areadetect.core.*;
import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.BoundryID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class BaseLineAreaDetect {

  /** 异常区域检测的滑动时间窗口 */
  SlidingEventTimeWindows detectWindowAssigner;

  /** 数据源，其中数据类型为{@link DetectUnit}. */
  SingleOutputStreamOperator<DetectUnit> source;


  protected GeometryKpiGetter geometryKpiGetter;
  double dist ;

  public BaseLineAreaDetect(SingleOutputStreamOperator<DetectUnit> source,
                    SlidingEventTimeWindows detectWindowAssigner,
                    GeometryKpiGetter geometryKpiGetter,
                    double dist) {
    this.source = source;
    this.detectWindowAssigner = detectWindowAssigner;
    this.geometryKpiGetter = geometryKpiGetter;
    this.dist = dist;
  }


  public DataStream<Geometry> process() {
    final OutputTag<Tuple3<BoundryID, DetectUnit, AreaID>> borderUnits = new OutputTag<Tuple3<BoundryID, DetectUnit, AreaID>>("border") { };
    final OutputTag<Tuple2<AreaID, List<DetectUnit>>> needCombineTag = new OutputTag<Tuple2<AreaID, List<DetectUnit>>>("need combine") { };
    SingleOutputStreamOperator<Tuple2<AreaID, List<DetectUnit>>> localDetectStream = localDetect(source);
    return combineAllGeom(localDetectStream, new GlobalAreaGeomGetter(geometryKpiGetter));
  }

  /**
   * 3路输出
   */
  private SingleOutputStreamOperator<Tuple2<AreaID, List<DetectUnit>>> localDetect(DataStream<DetectUnit> source) {
    return source
        .keyBy(DetectUnit::getId)
        .flatMap(new RedundantRouter())
        .keyBy(f -> f.f0)
        .window(detectWindowAssigner)
        .process(new BaseLineLocalAreaDetect());
  }

  private DataStream<Tuple2<AreaID, AreaID>> getFixRule(DataStream<Tuple3<BoundryID, DetectUnit, AreaID>> marginUnitStream) {
    return marginUnitStream.keyBy(r -> r.f0).window(detectWindowAssigner)
        .process(new LocalAreaLinkFinder())
        .windowAll(detectWindowAssigner)
        .process(new Local2GlobalMapRuleGetter());
  }

  DataStream<Geometry> combineAllGeom(SingleOutputStreamOperator<Tuple2<AreaID, List<DetectUnit>>> localAreaStream, GlobalAreaGeomGetter geomGetter) {
    return localAreaStream.windowAll(detectWindowAssigner)
        .aggregate(new Combine(dist))
        .flatMap(new FlatMapFunction<List<Geometry>, Geometry>() {
          @Override
          public void flatMap(List<Geometry> value, Collector<Geometry> out) throws Exception {
            for (Geometry g : value) {
              out.collect(g);
            }
          }
        }).returns(Geometry.class);
  }
}
