package cn.edu.whu.glink.areadetect.examples.func;

import cn.edu.whu.glink.areadetect.core.GeometryKpiGetter;
import cn.edu.whu.glink.areadetect.feature.DetectUnit;

import java.util.List;

public class AveGeomKPIGetter implements GeometryKpiGetter {
  @Override
  public Object get(List<DetectUnit> detectUnits) {
    int count = 0;
    double sum = 0;
    for (DetectUnit record : detectUnits) {
      sum += (Double) record.getVal();
      count++;
    }
    return sum / count;
  }
}
