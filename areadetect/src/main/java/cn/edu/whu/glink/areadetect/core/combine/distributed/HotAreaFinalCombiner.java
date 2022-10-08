package cn.edu.whu.glink.areadetect.core.combine.distributed;

import cn.edu.whu.glink.areadetect.core.HotAreaPropCalculator;
import cn.edu.whu.glink.areadetect.datatypes.AreaID;
import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;
import cn.edu.whu.glink.areadetect.datatypes.HotArea;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 获取最终区域的全局Polygon。userdata中为1. 区域ID 2. 时间 3. 平均值 4. 异常检测单元数量
 */
public class HotAreaFinalCombiner extends ProcessWindowFunction<HotArea, HotArea, AreaID, TimeWindow> {

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().getIndexOfThisSubtask();
  }

  @Override
  public void process(AreaID areaID, Context context, Iterable<HotArea> elements, Collector<HotArea> out) throws Exception {
    HotArea area = null;
    for (HotArea element : elements) {
      if (area == null) {
        area = element;
      } else {
        area.merge(element);
      }
    }
    assert area != null;
    // // calculate area kpi
    // int count = 0;
    // int sum = 0;
    // int pickUpNum = 0;
    // for(DetectUnit unit : detectUnits) {
    //   count ++;
    //   pickUpNum += unit.pickUpCount;
    //   sum += unit.pickUpCount + unit.dropDownCount;
    // }
    // double kpi = (double) sum / count;
    // double pickUpRatio = (double) pickUpNum / sum;
    // long ts = context.window().getEnd();
    // Instant i2 = Instant.ofEpochMilli(ts);
    // String time =  i2.toString();
    // // userdata中存储： 1. 区域ID 2. 时间 3. 平均值 4. 上客占比 5. 单元数量
    // Tuple tuple = new Tuple5<>(areaID, time, kpi, pickUpRatio, detectUnits.size());
    out.collect(area);
  }
}
