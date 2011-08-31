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

import java.io.IOException;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;

public class TripleFileStats {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {

    printFileStats("C:\\hdrs\\infobox_properties_en.tf");

  }

  
  public static void printFileStats(String file) throws IOException {
    
    TripleFileReader reader = new TripleFileReader(file);
    reader.open();
    
    System.out.println("Computing stats for file: " + reader.getFileInfo());
    System.out.println();
    
    TripleScanner scanner = reader.getScanner();
    
    long tripleCount = 0;
    
    long sBytes = 0;
    long pBytes = 0;
    long oBytes = 0;
    
    while(scanner.next()) {
      Triple t = scanner.pop();
      sBytes += t.getSubjectLength();
      pBytes += t.getPredicateLength();
      oBytes += t.getObjectLength();
      tripleCount++;
    }
    scanner.close();
    reader.close();
    
    
    System.out.println("Triples: " + tripleCount);
    if (0 < tripleCount) {
      System.out.println("Avg subject length: " + (sBytes/tripleCount));
      System.out.println("Avg predicate length: " + (pBytes/tripleCount));
      System.out.println("Avg object length: " + (oBytes/tripleCount));
    }
    
  }
  
}
