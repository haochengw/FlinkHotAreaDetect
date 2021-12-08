package cn.edu.whu.glink.areadetect.feature;

import java.util.Objects;

public class AreaID {
  long partitionID;
  long areaID;

  public AreaID(long partitionID, long areaID) {
    this.partitionID = partitionID;
    this.areaID = areaID;
  }

  public long getPartitionID() {
    return partitionID;
  }

  @Override
  public String toString() {
    return partitionID + "-" + areaID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AreaID areaID1 = (AreaID) o;
    return partitionID == areaID1.partitionID && areaID == areaID1.areaID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionID, areaID);
  }

}
