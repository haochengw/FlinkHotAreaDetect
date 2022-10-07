package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.AreaID;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

/**
 * 获取最终区域的全局Polygon。userdata中为1. 区域ID 2. 时间 3. 平均值 4. 异常检测单元数量
 */
public class GlobalAreaGeomGetter extends ProcessWindowFunction<Tuple2<AreaID, List<DetectUnit>>, Geometry, AreaID, TimeWindow> {

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final GeometryFactory gf = new GeometryFactory();
  private final GeometryKpiGetter kpiGetter;

  public GlobalAreaGeomGetter(GeometryKpiGetter kpiGetter) {
    this.kpiGetter = kpiGetter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public void process(AreaID areaID, Context context, Iterable<Tuple2<AreaID, List<DetectUnit>>> elements, Collector<Geometry> out) throws Exception {
    HashMap<Long, Polygon> polygonPool = new HashMap<>();
    List<DetectUnit> detectUnits = new LinkedList<>();
    // init polygon pool
    for (Tuple2<AreaID, List<DetectUnit>> element : elements) {
      List<DetectUnit> list = element.f1;
      for (DetectUnit d : list) {
          detectUnits.add(d);
          polygonPool.put(d.getId(), d.getPolygon());
      }
    }
    // union all polygons
    Geometry target = gf.createPolygon();
    for (Map.Entry<Long, Polygon> entry : polygonPool.entrySet()) {
      target = target.union(entry.getValue());
    }
    // calculate area kpi
    int count = 0;
    int sum = 0;
    int pickUpNum = 0;
    for(DetectUnit unit : detectUnits) {
      count ++;
      pickUpNum += unit.pickUpCount;
      sum += unit.pickUpCount + unit.dropDownCount;
    }
    double kpi = (double) sum / count;
    double pickUpRatio = (double) pickUpNum / sum;
    long ts = context.window().getEnd();
    Instant i2 = Instant.ofEpochMilli(ts);
    String time =  i2.toString();
    // userdata中存储： 1. 区域ID 2. 时间 3. 平均值 4. 上客占比 5. 单元数量
    Tuple tuple = new Tuple5<>(areaID, time, kpi, pickUpRatio, detectUnits.size());
    target.setUserData(tuple);
    out.collect(target);
  }

  protected Object getKPI(List<DetectUnit> detectUnits) {
    return kpiGetter.get(detectUnits);
  }
}
