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

import de.hpi.fgis.hdrs.Triple;

public class TripleSizer {

  static final int N = 1000;
  
  /**
   * A small tool for measuring/estimating the size of an triple object with no data.
   */
  public static void main(String[] args) {

    System.out.println("Estimating triple object size (no data) ...");
    System.out.println();
    
    Triple t = Triple.newInstance(new byte[0], 0, (short) 0, (short) 0, 0, 0);
    System.out.println("Triple.estimateSize() -> " + t.estimateSize() + " bytes");
    System.out.println();

    System.out.println("Measuring...");

    // shared buffer
    
    Triple[] triples = new Triple[N];    
    
    byte[] buf = null;
    
    long startMemoryUse = getMemoryUse();
    buf = new byte[0];
    for (int i = 0; i<N; ++i) {
      triples[i] = Triple.newInstance(buf, 0, (short) 0, (short) 0, 0, 0);
    }
    long endMemoryUse = getMemoryUse();

    long sz = ( endMemoryUse - startMemoryUse ) /N;

    System.out.println("Shared buffer: " + sz + " bytes = " + (sz/4) + " words");
    
    
    // private buffer
    
    triples = new Triple[N];
    
    startMemoryUse = getMemoryUse();
    for (int i = 0; i<N; ++i) {
      triples[i] = Triple.newInstance(new byte[0], 0, (short) 0, (short) 0, 0, 0);
    }
    endMemoryUse = getMemoryUse();

    sz = ( endMemoryUse - startMemoryUse ) /N;

    System.out.println("Private buffer: " + sz + " bytes = " + (sz/4) + " words");
    
  }
  
  

  private static long getMemoryUse(){
    putOutTheGarbage();
    long totalMemory = Runtime.getRuntime().totalMemory();

    putOutTheGarbage();
    long freeMemory = Runtime.getRuntime().freeMemory();

    return (totalMemory - freeMemory);
  }

  private static void putOutTheGarbage() {
    collectGarbage();
    collectGarbage();
  }

  private static void collectGarbage() {
    try {
      System.gc();
      Thread.currentThread();
      Thread.sleep(100);
      System.runFinalization();
      Thread.currentThread();
      Thread.sleep(100);
    }
    catch (InterruptedException ex){
      ex.printStackTrace();
    }
  }
  

}
