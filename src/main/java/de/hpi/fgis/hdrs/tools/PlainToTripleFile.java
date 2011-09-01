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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.parser.TripleParserFactory;
import de.hpi.fgis.hdrs.tio.FileSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSource;
import de.hpi.fgis.hdrs.tools.Utils.BandwidthResult;
import de.hpi.fgis.hdrs.tools.Utils.Timer;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileUtils;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;

public class PlainToTripleFile {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    
    if (2 != args.length) {
      System.out.println("usage: PlainToTripleFile plainfile triplefile");
      return;
    }
    
    convertTextToTripleFile(
      args[0],
      args[1],
      Triple.COLLATION.SPO,
      TripleFileWriter.DEFAULT_BLOCKSIZE,
      Compression.ALGORITHM.SNAPPY);
  }
  
  
  public static void convertTextToTripleFile(
      String textFilePath,
      String targetFilePath,
      Triple.COLLATION order,
      int blockSize,
      Compression.ALGORITHM compression) throws IOException {
   
    System.out.println("Staring file conversion:  Text file -> Triple file");
    System.out.println("  Input file : " + textFilePath);
    
    writeTripleFile(
        new FileSource(new File(textFilePath), TripleParserFactory.getDefaultLineParser()), 
        targetFilePath,
        order,
        blockSize,
        compression);
  }
  
  
  public static void writeTripleFile(
      TripleSource source,
      String targetFilePath,
      Triple.COLLATION order,
      int blockSize,
      Compression.ALGORITHM compression) throws IOException {
    
    final long nBytesPerSplit = 500 * 1024 * 1024; // 500 MB
    
    System.out.println("Writing triple file");
    System.out.println("  Output file: " + targetFilePath);
    System.out.println("  Order      : " + order);
    System.out.println("  Block size : " + blockSize);
    System.out.println("  Comprssion : " + compression);
    System.out.println("  Split size : " + Utils.toMB(nBytesPerSplit) + " MB");
    System.out.println();
    
    List<String> splits = new ArrayList<String>();
    NavigableSet<Triple> tree = new TreeSet<Triple>(order.comparator());
    
    System.out.println("Reading ...");
    TripleScanner scanner = source.getScanner();
    long nBytes = 0;
    while (scanner.next()) {
      Triple t = scanner.pop();
      tree.add(t);
      
      nBytes += t.serializedSize();
      
      if (nBytes > nBytesPerSplit) {
        // write out triples loaded
        File file = File.createTempFile("hdrs", "");
        file.deleteOnExit();
        splits.add(file.getAbsolutePath());
        
        System.out.print("  Writing out split file #" + splits.size() + "... ");
        BandwidthResult result = Utils.writeToFile(tree, file, order, blockSize, compression);        
        System.out.println("done. (" + result.totalBW() + ")");
        
        tree = new TreeSet<Triple>(order.comparator());
        nBytes = 0;
      }
    }
    scanner.close();
    System.out.println("Done reading.");
    
    if (splits.isEmpty()) {
      System.out.print("No file splits, writing target file...");
      BandwidthResult result = Utils.writeToFile(tree, 
          new File(targetFilePath), order, blockSize, compression);
      System.out.println("done. (" + result.totalBW() + ")");
    } else {
      // write out triples loaded
      if (0 < nBytes) {
        File file = File.createTempFile("hdrs", "");
        file.deleteOnExit();
        splits.add(file.getAbsolutePath());
      
        System.out.print("  Writing out last split file #" + splits.size() + "... ");
        BandwidthResult result = Utils.writeToFile(tree, file, order, blockSize, compression);
        System.out.println("done. (" + result.totalBW() + ")");
      
        tree = null;
      }
      
      // merge files
      System.out.print("Merging " + splits.size() + " splits into target file... ");
      
      TripleFileReader[] readers = TripleFileUtils.openAllFiles(splits.toArray(new String[0]));
      
      Timer timer = Utils.newTimer();
      timer.start();
      TripleFileInfo fileInfo = TripleFileUtils.merge(targetFilePath, 
          TripleFileWriter.DEFAULT_BLOCKSIZE, compression, readers);
      timer.stop();
      
      System.out.println("done. (" 
          + timer.formatBandwidth(fileInfo.getSize(), fileInfo.getUncompressedSize())
          + ")");
    }
    System.out.println();
    System.out.println("Quitting.");
  }
    
}
