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

package de.hpi.fgis.hdrs.triplefile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.MergedScanner;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * A container for a bunch of methods that come in handy when working
 * with triple files.
 * @author daniel.hefenbrock
 *
 */
public class TripleFileUtils {

  
  /**
   * Open all specified files
   * @param files  Files to open
   * @return  An array of readers for the files
   */
  public static TripleFileReader[] openAllFiles(String...files) {
    TripleFileReader[] readers = new TripleFileReader[files.length];
    for (int i=0; i<files.length; ++i) {
      readers[i] = new TripleFileReader(files[i]);
    }
    return readers;
  }
  
  /**
   * Copy a triple file.  The order will not change.
   * @param src  The source file
   * @param target  The target file (will be created)
   * @param blockSize  Block size of the new file
   * @param compression  Compression of the new file
   * @return File info for copied file
   * @throws IOException 
   * @throws IOException
   */
  public static TripleFileInfo copy(String src, String target, 
      int blockSize, Compression.ALGORITHM compression) throws IOException {
    return copy(src, target, blockSize, compression, 0);
  }
  
  
  public static TripleFileInfo copy(String src, String target, 
      int blockSize, Compression.ALGORITHM compression, int limit) throws IOException {
    
    TripleFileReader reader = new TripleFileReader(src);
    reader.open();
    
    TripleFileWriter writer = new TripleFileWriter(target, reader.getOrder(), 
        blockSize, compression);
    
    TripleScanner scanner = reader.getScanner();
    
    int cnt = 0;
    while (scanner.next() && (0 == limit || cnt++ < limit)) {
      writer.appendTriple(scanner.pop());
    }
    
    writer.close();
    reader.close();
    
    return writer.getFileInfo();
  }
  
  
  /**
   * Merge a number of triple files.  All files must have the same
   * collation order.  This method performs a merge-join of the 
   * input files that is written out to disk.
   * @param target  Target file path
   * @param blockSize  Target file block size
   * @param compression  Target file compression
   * @param srcReaders  Source file readers
   * @return  File info for target file
   * @throws IOException
   */
  public static TripleFileInfo merge(String target, int blockSize, 
      Compression.ALGORITHM compression, TripleFileReader...srcReaders) throws IOException {
    
    // first get a scanner for each file
    
    TripleScanner[] scanners = new TripleScanner[srcReaders.length];
    
    for (int i=0; i<srcReaders.length; ++i) {
      if (!srcReaders[i].isOpen()) {
        srcReaders[i].open();
      }
      scanners[i] = srcReaders[i].getScanner();
    }
    
    // create a merged scanner
    
    TripleScanner merged = new MergedScanner(scanners);
    
    // write out triples
    TripleFileWriter writer = new TripleFileWriter(target, scanners[0].getOrder(),
        blockSize, compression);
    
    while (merged.next()) {
      writer.appendTriple(merged.pop());
    }
    
    // done!  that was easy, wasn't it?!
    
    writer.close();
    merged.close();
    
    return writer.getFileInfo();
  }

  
  public static TripleFileInfo merge(String target, String...srcFiles) throws IOException {
    
    TripleFileReader[] readers = openAllFiles(srcFiles);
    
    return merge(target, TripleFileWriter.DEFAULT_BLOCKSIZE, 
        Compression.ALGORITHM.DEFLATE, readers);
  }
  
  
  /**
   * Load triples from the file into memory.
   * @param filename  The triple file to read from
   * @param limit  How many triples to read at most, 0 = read all
   * @return  The list of triples read in the file order
   * @throws IOException
   */
  public static List<Triple> loadTriples(
      String filename, int limit) throws IOException {
    
    TripleFileReader reader = new TripleFileReader(filename);
    reader.open();
    
    TripleScanner scanner = reader.getScanner();
    
    List<Triple> triples = new ArrayList<Triple>();
    
    if (0 >= limit) {
      limit = Integer.MAX_VALUE;
    }
    
    int cnt = 0;
    while (scanner.next()) {
      Triple t = scanner.pop();
      triples.add(t);
      
      if (limit <= ++cnt) {
        break;
      }
    }
    
    scanner.close();
    reader.close();
    
    return triples;
  }
  
  
}
