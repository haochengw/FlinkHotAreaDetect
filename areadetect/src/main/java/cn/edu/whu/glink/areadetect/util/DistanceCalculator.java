package cn.edu.whu.glink.areadetect.util;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public interface DistanceCalculator extends Serializable {

  double calcDistance(Geometry geom1, Geometry geom2);

  Envelope calcBoxByDist(Geometry geom, double distance);
}
