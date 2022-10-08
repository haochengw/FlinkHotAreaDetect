package cn.edu.whu.glink.areadetect.datatypes;

import java.util.Objects;

public class AreaID {
  long partitionID;
  long areaID;
  long timestamp;

  public AreaID(long partitionID, long areaID, long timestamp) {
    this.partitionID = partitionID;
    this.areaID = areaID;
    this.timestamp = timestamp;
  }

  public void setPartitionID(long partitionID) {
    this.partitionID = partitionID;
  }

  public long getAreaID() {
    return areaID;
  }

  public void setAreaID(long areaID) {
    this.areaID = areaID;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getPartitionID() {
    return partitionID;
  }

  @Override
  public String toString() {
    return "AreaID{" +
        "partitionID=" + partitionID +
        ", areaID=" + areaID +
        ", timestamp=" + timestamp +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AreaID areaID1 = (AreaID) o;
    return partitionID == areaID1.partitionID && areaID == areaID1.areaID && timestamp == areaID1.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionID, areaID, timestamp);
  }
}
