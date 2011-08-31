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

package de.hpi.fgis.hdrs.segment;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.benchmarks.BenchmarkUtils;
import de.hpi.fgis.hdrs.tio.NullSink;
import de.hpi.fgis.hdrs.tio.TripleGenerator;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;
import de.hpi.fgis.hdrs.tio.TripleSource;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;

public class BlockingTripleBufferTest {

  static int bufferSize = 200 * 1024 * 1024;
  static int dumpAt     = 150 * 1024 * 1024;
  
  static int nProducers = 2;
  
  /**
   * this is a little producer-consumer test program 
   * 
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    
    BlockingTripleBuffer buffer = new BlockingTripleBuffer(Triple.COLLATION.SPO, bufferSize, 0);
    
    TripleFileReader reader = new TripleFileReader("C:\\hdrs\\infobox_properties_en.tf");
    reader.open();
    
    Producer[] producers = new Producer[nProducers];
    for (int i=0; i<nProducers; ++i) {
      producers[i] = new Producer(buffer, reader);
    }
    
    Consumer consumer = new Consumer(buffer);
    
    Thread[] producerThreads = new Thread[nProducers];
    for (int i=0; i<nProducers; ++i) {
      producerThreads[i] = new Thread(producers[i]);
    }
    
    Thread consumerThread = new Thread(consumer);
    
    for (int i=0; i<nProducers; ++i) {
      producerThreads[i].start();
    }
    consumerThread.start();
    
    
    System.in.read();
    
    System.out.println("Stopping threads .... ");

    buffer.dumpSnapshot(new NullSink(), true);
    
    
    for (int i=0; i<nProducers; ++i) {
      producerThreads[i].interrupt();
    }
    
    
    for (int i=0; i<nProducers; ++i) {
      producerThreads[i].join();
    }
    consumerThread.join();
  }
  
  
  static class Producer implements Runnable {

    private final BlockingTripleBuffer buffer;
    private final TripleSource src;
       
    public Producer(BlockingTripleBuffer buffer) {
      this.buffer = buffer;
      this.src = new TripleGenerator();
    }
    
    public Producer(BlockingTripleBuffer buffer, TripleSource src) {
      this.buffer = buffer;
      this.src = src;
    }
    
    @Override
    public void run() {
      try {
        TripleScanner scanner = src.getScanner();
        while (scanner.next() && !buffer.isClosed()) {
          Triple t = scanner.pop();
          buffer.add(t);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } 
      System.out.println("Producer quitting");
    }
    
  }
  
  
  static class Consumer implements Runnable {

    private final BlockingTripleBuffer buffer;
        
    public Consumer(BlockingTripleBuffer buffer) {
      this.buffer = buffer;
    }
    
    @Override
    public void run() {
      while (!buffer.isClosed()) {
        int size = buffer.getSize();
        System.out.println();
        System.out.print("Size: " + BenchmarkUtils.formatMB(size));
        if (size < dumpAt) {
          System.out.println("   Sleeping ....");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            break;
          }
        } else {
          System.out.print("   Dumping triples ... ");
          try {
            File file = File.createTempFile("hdrs_test", "");
            file.deleteOnExit();
            TripleFileWriter writer = new TripleFileWriter(file.getAbsolutePath());
            BenchmarkUtils.Timer timer = new BenchmarkUtils.Timer();
            
            timer.start();
            buffer.dumpSnapshot(writer, false);
            writer.close();
            timer.stop();
            
            BenchmarkUtils.BandwidthResult res = new BenchmarkUtils.BandwidthResult(
                  writer.getFileInfo(), timer);
            System.out.println(res.totalBW());
          } catch (IOException e) {
            e.printStackTrace();
            return;
          }
          
          buffer.clearSnapshot();
        }
      }
      System.out.println("Consumer quitting");
    }
    
  }
  
  
  
  
  @Test
  public void testConcurrentWrites() throws InterruptedException, IOException {
    
    int nThreads = 10;
    int nTriples = 10000;
    
    TripleGenerator gen = new TripleGenerator();
    List<Triple> l1 = new ArrayList<Triple>();
    List<Triple> l2 = new ArrayList<Triple>();
    
    for (int i=0; i<nTriples; ++i) {
      Triple t = gen.generate();
      l1.add(t);
      Triple negativeTriple = Triple.newInstance(
          t.getBuffer(), t.getOffset(), t.getSubjectLength(), t.getPredicateLength(), 
          t.getObjectLength(), (short) -1);
      l2.add(negativeTriple);
    }
    Collections.shuffle(l1);
    Collections.shuffle(l2);
     
    
    BlockingTripleBuffer buf = new BlockingTripleBuffer(Triple.COLLATION.SPO, 1024 * 1024 * 1024, 0);
    buf.open();
    
    Thread[] threads = new Thread[nThreads];
    for (int i=0; i<nThreads; ++i) {
      threads[i] = new Thread(new TestThread(
          buf,
          (0 == i % 2) ? l1 : l2
          ));
      threads[i].start();
    }
    
    
    for (int i=0; i<nThreads; ++i) {
      threads[i].join();
    }
    
    NullSink ns = new NullSink();
    
    buf.dumpSnapshot(ns, false);
    
    assertEquals(0, ns.written());
    
  }

  
  static class TestThread implements Runnable {

    private final TripleSink sink;
    private final List<Triple> triples;
    
    public TestThread(TripleSink s, List<Triple> t) {
      sink = s;
      triples = t;
    }
    
    @Override
    public void run() {
      for (Triple t : triples) {
        //System.out.println("Thread " + this + ": put " + t);
        try {
          sink.add(t.clone());
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
      }
    }
    
  }
  
  
  
  
}
