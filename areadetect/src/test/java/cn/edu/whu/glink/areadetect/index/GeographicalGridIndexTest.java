package cn.edu.whu.glink.areadetect.index;

import junit.framework.TestCase;

/**
 * @author Haocheng Wang
 * Created on 2022/10/7
 */
public class GeographicalGridIndexTest extends TestCase {

  public void testKRing() {
    GeographicalGridIndex index = new GeographicalGridIndex(10);
    index.getIndex(20.0, 20.0);
    System.out.println(index.kRing(index.getIndex(20.0, 20.0), 1));
  }
}