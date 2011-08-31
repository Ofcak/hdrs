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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.NavigableSet;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class FileTransferTest {

  
  @Test
  public void testTransfer() throws IOException {
    
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
    
    
    FileStreamer streamer = reader.getStreamer();
    
    File file2 = File.createTempFile("hdrs_test", "");
    file2.deleteOnExit();
    
    TripleFileImporter importer = new TripleFileImporter(file2.getAbsolutePath(), 
        Triple.COLLATION.SPO,
        Compression.ALGORITHM.SNAPPY);
    
    FileBlock block;
    while (null != (block = streamer.nextBlock())) {
      importer.addBlocks(new FileBlock[]{block});
    }
    
    importer.close(streamer.getLastTriple());
    
    TripleFileReader reader2 = new TripleFileReader(file2);
    reader2.open();
    
    TripleScanner scanner = reader.getScanner();
    TripleScanner scanner2 = reader2.getScanner();
    
    while(scanner.next()) {
      assertTrue(scanner2.next());
      assertEquals(scanner.pop(), scanner2.pop());
    }
    
    assertFalse(scanner2.next());
    
    reader.close();
    reader2.close();
  }
  
  
  @Test
  public void testHalfFileTransfer() throws IOException {
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
        
    HalfFileReader topReader = new HalfFileReader(reader,
        reader.getSplitTriple(), HalfFileReader.HALF.TOP);
    topReader.open();
    
    TripleFileReader topReader2 = importFile(topReader);
    TripleScanner hscanner = topReader.getScanner();
    TripleScanner hscanner2 = topReader2.getScanner();
    while (hscanner.next()) {
      assertTrue(hscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), hscanner2.pop()));
    }
    assertTrue(!hscanner2.next());
    
    
    hscanner.close();
    hscanner2.close();
    topReader.close();
    topReader2.close();

    
    HalfFileReader bottomReader = new HalfFileReader(reader,
        reader.getSplitTriple(), HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
    
    TripleFileReader bottomReader2 = importFile(bottomReader);
    TripleScanner bscanner = bottomReader.getScanner();
    TripleScanner bscanner2 = bottomReader2.getScanner();
    while (bscanner.next()) {
      assertTrue(bscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          bscanner.pop(), bscanner2.pop()));
    }
    assertTrue(!bscanner2.next());
    
    bscanner.close();
    bscanner2.close();
    
    bottomReader.close();
    bottomReader2.close();
    
    
    reader.close();
  }
  
  
  @Test
  public void testHalfFileTransfer2() throws IOException {
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
    
    // get another split triple that is not the first triple of some block
    TripleScanner scanner = reader.getScanner(reader.getSplitTriple());
    for (int i=0; i<13; ++i) {
      scanner.next(); scanner.pop();
    }
    scanner.next();
    Triple t = scanner.pop();
    scanner.close();
    
    HalfFileReader topReader = new HalfFileReader(reader,
        t, HalfFileReader.HALF.TOP);
    
    topReader.open();
    
    TripleFileReader topReader2 = importFile(topReader);
    TripleScanner hscanner = topReader.getScanner();
    TripleScanner hscanner2 = topReader2.getScanner();
    while (hscanner.next()) {
      assertTrue(hscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), hscanner2.pop()));
    }
    assertTrue(!hscanner2.next());
    
    
    hscanner.close();
    hscanner2.close();
    topReader.close();
    topReader2.close();

    
    HalfFileReader bottomReader = new HalfFileReader(reader,
        t, HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
    
    TripleFileReader bottomReader2 = importFile(bottomReader);
    TripleScanner bscanner = bottomReader.getScanner();
    TripleScanner bscanner2 = bottomReader2.getScanner();
    while (bscanner.next()) {
      assertTrue(bscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          bscanner.pop(), bscanner2.pop()));
    }
    assertTrue(!bscanner2.next());
    
    bscanner.close();
    bscanner2.close();
    
    bottomReader.close();
    bottomReader2.close();
    
    
    reader.close();
  }
  
  
  
  @Test
  public void testHalfFileTransfer3() throws IOException {
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
    
    // get another split triple that is not the first triple of some block
    TripleScanner scanner = reader.getScanner(reader.getSplitTriple());
    for (int i=0; i<13; ++i) {
      scanner.next(); scanner.pop();
    }
    scanner.next();
    
    // modify this triple such that it is not present in the file (hopefully)
    Triple t = scanner.pop();
    t.getBuffer()[t.getObjectOffset()+t.getObjectLength()-1] += 8;
    scanner.close();
    
    
    HalfFileReader topReader = new HalfFileReader(reader,
        t, HalfFileReader.HALF.TOP);
    
    topReader.open();
    
    TripleFileReader topReader2 = importFile(topReader);
    TripleScanner hscanner = topReader.getScanner();
    TripleScanner hscanner2 = topReader2.getScanner();
    while (hscanner.next()) {
      assertTrue(hscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), hscanner2.pop()));
    }
    assertTrue(!hscanner2.next());
    
    
    hscanner.close();
    hscanner2.close();
    topReader.close();
    topReader2.close();

    
    HalfFileReader bottomReader = new HalfFileReader(reader,
        t, HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
    
    TripleFileReader bottomReader2 = importFile(bottomReader);
    TripleScanner bscanner = bottomReader.getScanner();
    TripleScanner bscanner2 = bottomReader2.getScanner();
    while (bscanner.next()) {
      assertTrue(bscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          bscanner.pop(), bscanner2.pop()));
    }
    assertTrue(!bscanner2.next());
    
    bscanner.close();
    bscanner2.close();
    
    bottomReader.close();
    bottomReader2.close();
    
    
    reader.close();
  }
  
  
  
  @Test
  public void testHalfFileTransfer4() throws IOException {
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    // take a triple smaller than everything in the file
    Triple t = Triple.newPattern("a", "a", "a");
    
    HalfFileReader topReader = new HalfFileReader(file.getAbsolutePath(),
        t, HalfFileReader.HALF.TOP);
    topReader.open();
    
    assertTrue(null == importFile(topReader));
    topReader.close();

    HalfFileReader bottomReader = new HalfFileReader(file.getAbsolutePath(),
        t, HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
    
    TripleFileReader bottomReader2 = importFile(bottomReader);
    TripleScanner bscanner = bottomReader.getScanner();
    TripleScanner bscanner2 = bottomReader2.getScanner();
    while (bscanner.next()) {
      assertTrue(bscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          bscanner.pop(), bscanner2.pop()));
    }
    assertTrue(!bscanner2.next());
    
    bscanner.close();
    bscanner2.close();
    
    bottomReader.close();
    bottomReader2.close();
    
    
  }
  
  
  
  @Test
  public void testHalfFileTransfer5() throws IOException {
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
    
    // take a triple greater than everything in the file
    Triple t = Triple.newPattern("x", "a", "a");
    
    HalfFileReader topReader = new HalfFileReader(file.getAbsolutePath(),
        t, HalfFileReader.HALF.TOP);
    topReader.open();
    
    TripleFileReader topReader2 = importFile(topReader);
    TripleScanner hscanner = topReader.getScanner();
    TripleScanner hscanner2 = topReader2.getScanner();
    while (hscanner.next()) {
      assertTrue(hscanner2.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), hscanner2.pop()));
    }
    assertTrue(!hscanner2.next());
    hscanner.close();
    hscanner2.close();
    topReader.close();
    topReader2.close();
    
    
    HalfFileReader bottomReader = new HalfFileReader(file.getAbsolutePath(),
        t, HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
    
    assertTrue( null == importFile(bottomReader));
    
    bottomReader.close();
    
  }
  
  
  
  TripleFileReader importFile(TripleFileReader source) throws IOException {
    FileStreamer streamer = source.getStreamer();
    
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    TripleFileImporter importer = new TripleFileImporter(file.getAbsolutePath(), 
        Triple.COLLATION.SPO,
        Compression.ALGORITHM.SNAPPY);
    
    FileBlock block;
    while (null != (block = streamer.nextBlock())) {
      importer.addBlocks(new FileBlock[]{block});
    }
    
    if (!importer.close(streamer.getLastTriple())) {
      return null;
    }
    
    TripleFileReader reader = new TripleFileReader(file);
    reader.open();
       
    return reader;
  }
  
  
}
