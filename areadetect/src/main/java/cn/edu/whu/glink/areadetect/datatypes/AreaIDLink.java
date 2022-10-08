package cn.edu.whu.glink.areadetect.datatypes;

import cn.edu.whu.glink.areadetect.core.combine.distributed.LocalAreaLinkFinder;

import java.util.Objects;

/**
 * @author Haocheng Wang
 * Created on 2022/10/7
 */
public class AreaIDLink {
  AreaID smaller;
  AreaID bigger;

  public AreaIDLink(AreaID localAreaID1, AreaID localAreaID2) {
    long p1 = localAreaID1.getPartitionID();
    long p2 = localAreaID2.getPartitionID();
    if (p1 < p2) {
      smaller = localAreaID1;
      bigger = localAreaID2;
    } else {
      smaller = localAreaID2;
      bigger = localAreaID1;
    }
  }

  public AreaID getSmaller() {
    return smaller;
  }

  public void setSmaller(AreaID smaller) {
    this.smaller = smaller;
  }

  public AreaID getBigger() {
    return bigger;
  }

  public void setBigger(AreaID bigger) {
    this.bigger = bigger;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AreaIDLink that = (AreaIDLink) o;
    return (smaller == that.smaller && bigger == that.bigger);
  }

  @Override
  public int hashCode() {
    return Objects.hash(smaller, bigger);
  }
}
