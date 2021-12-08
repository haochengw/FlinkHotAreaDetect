package cn.edu.whu.glink.areadetect.examples.IO.source.helper;


import cn.edu.whu.glink.index.GeographicalGridIndex;

public class MseString2GridDetectionUnit extends AbstractMseString2DU {

  private final GeographicalGridIndex geographicalGridIndex;


  public MseString2GridDetectionUnit(GeographicalGridIndex geographicalGridIndex) {
    this.geographicalGridIndex = geographicalGridIndex;
  }

  @Override
  public long getId(String[] vals) {
    double lng = Long.parseLong(vals[3]) / 1000000d;
    double lat = Long.parseLong(vals[4]) / 1000000d;
    return geographicalGridIndex.getIndex(lng, lat);
  }
}
