package cn.edu.whu.glink.areadetect.feature;

import java.util.Objects;

/**
 * 边界ID唯一标识两个邻接分区所形成的边界。
 */
public class BoundryID {
  /** 组成该边界的两个分区中值更小的分区ID */
  long smaller;

  /** 组成该边界的两个分区中值更大的分区ID */
  long bigger;

  public BoundryID(long a, long b) {
    if (a > b) {
      bigger = a;
      smaller = b;
    } else {
      bigger = b;
      smaller = a;
    }
  }

  public long getSmaller() {
    return smaller;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BoundryID boundryID = (BoundryID) o;
    return smaller == boundryID.smaller && bigger == boundryID.bigger;
  }

  @Override
  public int hashCode() {
    return Objects.hash(smaller, bigger);
  }
}
