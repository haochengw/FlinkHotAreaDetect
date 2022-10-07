package cn.edu.whu.glink.areadetect.examples.IO.source.helper;

/**
 * Mse data format : 2021-10-11 18:06:56 291 114000000 22539000 107 \N
 */
public class MseString2StationDetectionUnit extends AbstractMseString2DU {
  @Override
  protected long getId(String[] vals) {
    return Long.parseLong(vals[5]);
  }
}
