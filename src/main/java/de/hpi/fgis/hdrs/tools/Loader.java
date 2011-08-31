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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.LogFormatUtil;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.client.TripleOutputStream;
import de.hpi.fgis.hdrs.parser.BTCParser;
import de.hpi.fgis.hdrs.tio.FileSource;
import de.hpi.fgis.hdrs.tio.StreamScanner;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;

/**
 * Util for loading triples into HDRS. Input formats are flat files, triple files,
 * directories of files/triple files.  Also supports compression.
 * 
 * @author daniel.hefenbrock
 *
 */
public class Loader {
  
  static final Log LOG = LogFactory.getLog(Loader.class);
  static final String usage = "Load <peer address> [dbqztc] <filename/dirname>";

  public static void main(String[] args) throws IOException {
    
    if (2 > args.length) {
      System.out.println(usage);
      System.exit(1);
    }
    
    if (0 > args[0].indexOf(':')) {
      args[0] += ":" + Configuration.DEFAULT_RPC_PORT;
    }
    Configuration conf = Configuration.create();
    Client client = new Client(conf, args[0]);
    
    File[] files;
    String options = "";
    if (args[1].startsWith("-")) {
      options = args[1];
      if (3 > args.length) {
        System.out.println(usage);
        System.exit(1);
      }
      if (0 < options.indexOf('d')) {
        File dir = new File(args[2]);
        if (!dir.isDirectory()) {
          throw new IOException("Directory does not exist.");
        }
        files = dir.listFiles();
      } else {
        files = new File[]{new File(args[2])};
      }
    } else {
      files = new File[]{new File(args[1])};
    }
    
    boolean quiet = 0 < options.indexOf('q');
    boolean context = 0 < options.indexOf('c');
    
    boolean bench = 0 < options.indexOf('b');
    List<BenchSample> benchSamples = null;
    if (bench) {
      benchSamples = new ArrayList<BenchSample>();
    }
    
    long timeStalled = 0;
    long timeRouterUpdate = 0;
    long abortedTransactions = 0;
    
    long nBytesTotal = 0;
    long nTriplesTotal = 0;
    long timeTotal = 0;
    
    for (int i=0; i<files.length; ++i) {
      Closeable source = null;
      TripleScanner scanner = null;
    
      try {
        if (0 < options.indexOf('t')) {
          TripleFileReader reader = new TripleFileReader(files[i]);
          reader.open();
          scanner = reader.getScanner();
          source = reader;
        } else if (0 < options.indexOf('z')) { 
          GZIPInputStream stream = new GZIPInputStream(new FileInputStream(files[i]));
          BTCParser parser = new BTCParser();
          parser.setSkipContext(!context);
          scanner = new StreamScanner(stream, parser);
          source = stream;
        } else {
          BTCParser parser = new BTCParser();
          parser.setSkipContext(!context);
          FileSource file = new FileSource(files[i], parser);
          scanner = file.getScanner();
          source = file;
        }
      } catch (IOException ioe) {
        System.out.println("Error: Couldn't open " + files[i] + ". See log for details.");
        LOG.error("Error: Couldn't open " + files[i] + ":", ioe);
        continue;
      }
      
      long nBytes = 0;
      long nTriples = 0;
      long time = System.currentTimeMillis();
    
      TripleOutputStream out = client.getOutputStream();
    
      while (scanner.next()) {
        Triple t = scanner.pop();
        out.add(t);
        nBytes += t.serializedSize();
        nTriples++;
        
        if (!quiet && 0 == (nTriples % (16* 1024))) {
          System.out.print(String.format("\rloading... %d triples (%.2f MB, %.2f MB/s)", 
              nTriples, LogFormatUtil.MB(nBytes), LogFormatUtil.MBperSec(nBytes, 
                  System.currentTimeMillis() - time)));
        }
      }
      out.close();
      
      time = System.currentTimeMillis() - time;
      
      scanner.close();
      source.close();
      
      if (!quiet) {
        System.out.print("\r");
      }
      System.out.println(String.format("%s: %d triples (%.2f MB) loaded " +
      		"in %.2f seconds (%.2f MB/s)",
      		files[i],
          nTriples, 
          LogFormatUtil.MB(nBytes), 
          time/1000.0, 
          LogFormatUtil.MBperSec(nBytes, time)));
      
      nBytesTotal += nBytes;
      nTriplesTotal += nTriples;
      timeTotal += time;
      
      timeStalled += out.getTimeStalled();
      timeRouterUpdate += out.getTimeRouterUpdate();
      abortedTransactions += out.getAbortedTransactions();
      
      if (bench) {
        benchSamples.add(new BenchSample(time, nTriples, nBytes));
      }
    }
    
    client.close();
    
    if (0 == nTriplesTotal) {
      System.out.println("No triples loaded.");
      return;
    }
    
    System.out.println(String.format("Done loading.  Totals: %d triples (%.2f MB) loaded " +
        "in %.2f seconds (%.2f MB/s)",
        nTriplesTotal, 
        LogFormatUtil.MB(nBytesTotal), 
        timeTotal/1000.0, 
        LogFormatUtil.MBperSec(nBytesTotal, timeTotal)));
    
    System.out.println(String.format("  Client stats.  Stalled: %.2f s  RouterUpdate: %.2f s" +
    		"  AbortedTransactions: %d", timeStalled/1000.0, timeRouterUpdate/1000.0, 
    		abortedTransactions));
    
    if (bench) {
      System.out.println();
      System.out.println("Benchmark Samples:");
      System.out.println("time\tsum T\tsum MB\tMB/s");
      System.out.println(String.format("%.2f\t%d\t%.2f\t%.2f", 0f, 0, 0f, 0f));
      long time = 0, nTriples = 0, nBytes = 0;
      for (BenchSample sample : benchSamples) {
        time += sample.time;
        nTriples += sample.nTriples;
        nBytes += sample.nBytes;
        System.out.println(String.format("%.2f\t%d\t%.2f\t%.2f",
            time / 1000.0, nTriples, LogFormatUtil.MB(nBytes),
            LogFormatUtil.MBperSec(sample.nBytes, sample.time)));
      }
    }
  }

  private static class BenchSample {
    public BenchSample(long time, long nTriples, long nBytes) {
      this.time = time;
      this.nTriples = nTriples;
      this.nBytes = nBytes;
    }
    long time;
    long nTriples;
    long nBytes;
  }
  
}
