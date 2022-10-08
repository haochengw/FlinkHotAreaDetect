package cn.edu.whu.glink.areadetect.core;

import cn.edu.whu.glink.areadetect.datatypes.DetectUnit;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public interface HotAreaPropCalculator extends Serializable {
  Object get(Set<DetectUnit> detectUnits);
}
