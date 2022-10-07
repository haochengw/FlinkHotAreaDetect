package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;

import java.io.Serializable;
import java.util.List;

public interface GeometryKpiGetter extends Serializable {
  Object get(List<DetectUnit> detectUnits);
}
