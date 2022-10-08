package cn.edu.whu.glink.areadetect.datatypes;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Haocheng Wang
 * Created on 2022/10/7
 */
public class HotArea {
  static final Logger logger = LoggerFactory.getLogger(HotArea.class);
  private static final GeometryFactory gf = new GeometryFactory();
  private Set<DetectUnit> detectUnits;
  private AreaID areaID;
  private long timestamp;

  public HotArea(Set<DetectUnit> set, AreaID areaID) {
    this.detectUnits = set;
    this.areaID = areaID;
    timestamp = areaID.timestamp;
  }

  public void setDetectUnits(Set<DetectUnit> detectUnits) {
    this.detectUnits = detectUnits;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Set<DetectUnit> getDetectUnits() {
    return detectUnits;
  }

  public AreaID getAreaID() {
    return areaID;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setAreaID(AreaID areaID) {
    this.areaID = areaID;
  }

  public Geometry getGeometry() {
    Geometry target = gf.createPolygon();
    for (DetectUnit du : detectUnits) {
      target.union(du.getPolygon());
    }
    target.setUserData(this);
    return target;
  }

  public void merge(HotArea area) {
    if (timestamp == area.getTimestamp() && !areaID.equals(area.areaID)) {
      detectUnits.addAll(area.getDetectUnits());
    } else if (timestamp != area.getTimestamp()){
      String msg = String.format("合并的两个热点区域的时间不同,主体区域的时间为%s, 参数区域的时间为%s",
          timestamp, area.getTimestamp());
      logger.error(msg);
      throw new UnsupportedOperationException(msg);
    } else if (areaID.equals(area.areaID)) {
      String msg = String.format("合并的两个热点区域ID相同, 无法合并, ID: %s", areaID);
      logger.error(msg);
      throw new UnsupportedOperationException(msg);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HotArea area = (HotArea) o;
    return Objects.equals(areaID, area.areaID) && area.getTimestamp() == timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(areaID);
  }
}
