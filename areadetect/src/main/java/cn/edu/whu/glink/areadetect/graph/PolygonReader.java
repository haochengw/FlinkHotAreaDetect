package cn.edu.whu.glink.areadetect.graph;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class PolygonReader {

  private static final WKTReader wktReader = new WKTReader(new GeometryFactory());

  public static HashMap<Long, Polygon> read(String polygonFilePath) {
    HashMap<Long, Polygon> res = new HashMap<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(polygonFilePath));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\\|");
        Long id = Long.parseLong(tokens[0]);
        Polygon polygon = (Polygon) wktReader.read(tokens[1]);
        res.put(id, polygon);
      }
      br.close();

    } catch (IOException | ParseException e) {
      e.printStackTrace();
    }
    return res;
  }
}
