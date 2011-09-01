/**
 * Copyright 2011 Daniel Hefenbrock
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.hpi.fgis.hdrs.tools;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;


public class Utils {

  public static Timer newTimer() {
    return new Timer();
  }
  
  public static Timer newMinTimer() {
    return new MinTimer();
  }
  
  
  public static class Timer {
    long time;
    public void start() {
      time = System.currentTimeMillis();
    }
    public void stop() {
      time = System.currentTimeMillis() - time;
    }
    public long getTime() {
      return time;
    }
    public double getSeconds() {
      return time/1000.0;
    }
    public String getFormattedTime() {
      return String.format("%.3f", getSeconds());
    }
    public String formatBandwidth(long fileBytes, long uncompressedBytes) {
      return String.format("%.2f MB (%.2f MB), %.3f s, %.2f MB/s (eff.), %.2f MB/s (I/O)", 
          toMB(fileBytes),
          toMB(uncompressedBytes),
          getSeconds(),
          toMB(uncompressedBytes) / getSeconds(),
          toMB(fileBytes) / getSeconds()
          );
    }
  }
  
  public static class MinTimer extends Timer {
    @Override
    public void stop() {
      long newtime = System.currentTimeMillis() - time;
      if (newtime < time) {
        time = newtime;
      }
    }
  }
  
  public static class BandwidthResult {
    private final TripleFileInfo fileInfo;
    private final Timer timer;
    
    public BandwidthResult(TripleFileInfo fileInfo, Timer timer) {
      this.fileInfo = fileInfo;
      this.timer = timer;
    }
    
    public String totalBW() {
      return timer.formatBandwidth(
          fileInfo.getSize(), fileInfo.getUncompressedSize());
    }
 
    public String dataBW() {
      return timer.formatBandwidth(
          fileInfo.getSize(), fileInfo.getUncompressedDataSize());
    } 
   
  }
  
  
  public static double toMB(long nbytes) {
    return nbytes/(1024.0*1024.0);
  }
  public static double toMB(int nbytes) {
    return nbytes/(1024.0*1024.0);
  }
  
  public static String formatMB(int nbytes) {
    return String.format("%.2f", toMB(nbytes));
  }
  
  public static String formatMB(long nbytes) {
    return String.format("%.2f", toMB(nbytes));
  }
  
  
  
  public static BandwidthResult writeToFile(
      Collection<Triple> triples, 
      File file, 
      Triple.COLLATION order, 
      int blockSize,
      Compression.ALGORITHM compression) throws IOException {
    
    TripleFileWriter writer = 
      new TripleFileWriter(
          file.getAbsolutePath(), order, blockSize,
          compression);
    
    return writeToFile(triples, writer);
  }
  
  public static BandwidthResult writeToFile(Collection<Triple> triples,
      TripleFileWriter writer) throws IOException {
    
    Timer timer = new MinTimer();
    
    timer.start();
    for (Triple t : triples) {
      writer.appendTriple(t);
    } 
    writer.close();
    timer.stop();
    
    return new BandwidthResult(writer.getFileInfo(), timer);
  }
  
  public static BandwidthResult loadFromFile(Collection<Triple> triples,
      TripleFileReader reader) throws IOException {
    
    if (!reader.isOpen()) {
      reader.open();
    }
    
    TripleScanner scanner = reader.getScanner();
    
    Timer timer = new MinTimer();
    
    timer.start();
    while (scanner.next()) {
      Triple t = scanner.pop();
      if (null != triples) {
        triples.add(t);
      }
    }
    timer.stop();
    
    return new BandwidthResult(reader.getFileInfo(), timer);
  }
  
}
