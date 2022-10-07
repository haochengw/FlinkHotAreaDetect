package cn.edu.whu.glink.areadetect.graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class NeighborReader {

  public static HashMap<Long, HashSet<Long>> read(String neighborFilePath) {
    HashMap<Long, HashSet<Long>> res = new HashMap<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(neighborFilePath));
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        Long id = Long.parseLong(tokens[0]);
        HashSet<Long> neighborSet = new HashSet<>(tokens.length - 1);
        for(int i = 1; i < tokens.length; i++) {
          neighborSet.add(Long.parseLong(tokens[i]));
        }
        res.put(id, neighborSet);
      }
      br.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
    return res;
  }
}
