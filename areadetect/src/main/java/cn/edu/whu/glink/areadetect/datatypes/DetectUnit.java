package cn.edu.whu.glink.areadetect.datatypes;

import org.locationtech.jts.geom.Polygon;

import java.util.Objects;
import java.util.Set;

public class DetectUnit {
  private Long id;
  private Long timestamp;
  private int val;
  long mainPartition;
  Set<Long> nearByUnitsID;
  Set<Long> nearByPartitions;
  Polygon polygon;

  public DetectUnit(Long id, Long timestamp, int val) {
    this.id = id;
    this.timestamp = timestamp;
    this.val = val;
  }

  public Long getMainPartition() {
    return mainPartition;
  }

  public int getVal() {
    return val;
  }

  public void setVal(int val) {
    this.val = val;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getId() {
    return id;
  }

  public DetectUnit setId(Long id) {
    this.id = id;
    return this;
  }

  public Set<Long> getNearByUnitsID() {
    return nearByUnitsID;
  }

  public Polygon getPolygon() {
    return polygon;
  }

  public Set<Long> getNearByPartitions() {
    return nearByPartitions;
  }

  public void setMainPartition(long mainPartition) {
    this.mainPartition = mainPartition;
  }

  public void setNearByUnitsID(Set<Long> nearByUnitsID) {
    this.nearByUnitsID = nearByUnitsID;
  }

  public void setNearByPartitions(Set<Long> nearByPartitions) {
    this.nearByPartitions = nearByPartitions;
  }

  public void setPolygon(Polygon polygon) {
    this.polygon = polygon;
  }

  @Override
  public String toString() {
    return "DetectUnit{"
        + "id=" + id
        + ", timestamp=" + timestamp
        + ", val=" + val
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DetectUnit that = (DetectUnit) o;
    return Objects.equals(id, that.id) && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp, val);
  }
}
