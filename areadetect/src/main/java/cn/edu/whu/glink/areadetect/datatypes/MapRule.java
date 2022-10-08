package cn.edu.whu.glink.areadetect.datatypes;

import java.awt.geom.Area;

/**
 * @author Haocheng Wang
 * Created on 2022/10/7
 */
public class MapRule {
  AreaID from;
  AreaID to;

  public MapRule(AreaID from, AreaID to) {
    this.from = from;
    this.to = to;
  }

  public AreaID getTo() {
    return to;
  }

  public void setTo(AreaID to) {
    this.to = to;
  }

  public AreaID getFrom() {
    return from;
  }

  public void setFrom(AreaID from) {
    this.from = from;
  }
}
