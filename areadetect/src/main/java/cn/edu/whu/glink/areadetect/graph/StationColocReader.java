package cn.edu.whu.glink.areadetect.graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


public class StationColocReader {
  /**
   * 获取位置相同的点间的映射关系，同一经纬度的所有点ID将被映射为同一个点的ID。
   */
  public static HashMap<Long, Long> read(String fp) {
    HashMap<Long, Long> res = new HashMap<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(fp));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        int size = tokens.length;
        if (size > 1) {
          long mainStation = Long.parseLong(tokens[0]);
          for(int i = 1; i < size; i++) {
            res.put(Long.parseLong(tokens[i]), mainStation);
          }
        }
      }
      br.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
    return res;
  }
}
