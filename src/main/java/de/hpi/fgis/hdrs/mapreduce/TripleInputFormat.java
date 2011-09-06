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

package de.hpi.fgis.hdrs.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class TripleInputFormat extends InputFormat<NullWritable, Triple> implements Configurable {

  private final Log LOG = LogFactory.getLog(TripleInputFormat.class);
  
  public static final String STORE_ADDRESS = "hdrs.mapreduce.store";
  public static final String INPUT_INDEX = "hdrs.mapreduce.inputindex";
  public static final String AGGREGATION_LEVEL = "hdrs.mapreduce.aggregationlevel";
  
  public static final String PATTERN_SUBJ = "hdrs.mapreduce.patternsubj";
  public static final String PATTERN_PRED = "hdrs.mapreduce.patternpred";
  public static final String PATTERN_OBJ = "hdrs.mapreduce.patternobj";
  
  
  private Configuration conf = null;
  private Client client = null;
  
  @Override
  public void setConf(org.apache.hadoop.conf.Configuration conf) {
    this.conf = new Configuration(conf);
    try {
      client = new Client(this.conf, conf.get(STORE_ADDRESS));
    } catch (IOException ex) {
      LOG.error("Couldn't connect to HDRS store.", ex);
    }
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  protected void finalize() throws Throwable {
    try {
      if (null != client) {
        client.close();
      }
    } finally {
      super.finalize();
    }
  }
  
  /**
   * Set the address and port of the HDRS store to read from
   * for the provided job.
   * @param job
   * @param address An address string of the form "address[:port]"
   */
  public static void setStoreAddress(Job job, String address) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.set(STORE_ADDRESS, address);
  }
  
  /**
   * <p>Set the index to read from for the provided job.
   * <p>Note the index must be present in the HDRS store.
   * @param job
   * @param index  The index (e.g. "SPO")
   */
  public static void setIndex(Job job, String index) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.set(INPUT_INDEX, index);
  }
  
  /**
   * <p>Set the aggregation level "2" for logical input splits.
   * This means, given SPO collation, triple with the same subject S
   * and predicate P are guaranteed to fall into one input split 
   * (that is, they are not scattered across several input splits).
   * @param job
   */
  public static void setAggregationLevel2(Job job) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.setInt(AGGREGATION_LEVEL, 2);
  }
  
  /**
   * <p>Set the aggregation level "1" for logical input splits.
   * This means, given SPO collation, triple with the same subject S
   * are guaranteed to fall into one input split (that is, they
   * are not scattered across several input splits).
   * @param job
   */
  public static void setAggregationLevel1(Job job) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.setInt(AGGREGATION_LEVEL, 1);
  }
  
  /**
   * <p>Disable input split triple aggregation (no "logical splits).
   * This means the index (or index part) to be read is broken up 
   * at arbitrary positions (at segment borders). 
   * <p>This is the default for HDRS MapReduce jobs.
   * @param job
   */
  public static void setNoAggregation(Job job) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.setInt(AGGREGATION_LEVEL, 3);
  }
  
  /**
   * Set a pattern to be matched when reading the index of this
   * job.
   * @param job
   * @param pattern  The triple pattern to be matched.
   */
  public static void setPattern(Job job, Triple pattern) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    String s = pattern.getSubject();
    String p = pattern.getPredicate();
    String o = pattern.getObject();
    if (null != s) conf.set(PATTERN_SUBJ, s);
    if (null != p) conf.set(PATTERN_PRED, p);
    if (null != o) conf.set(PATTERN_OBJ, o);
  }
  
  
  private static class TripleReader extends RecordReader<NullWritable, Triple> {

    private final TripleScanner scanner;
    
    TripleReader(TripleScanner scanner) {
      this.scanner = scanner;
    }
    
    @Override
    public void close() throws IOException {
      scanner.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public Triple getCurrentValue() throws IOException, InterruptedException {
      return scanner.peek();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {      
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (null != scanner.peek()) {
        scanner.pop();
      }
      return scanner.next();
    }
    
  }
  
  
  @Override
  public RecordReader<NullWritable, Triple> createRecordReader(InputSplit s,
      TaskAttemptContext context) throws IOException, InterruptedException {
    
    IndexSplit split = (IndexSplit) s;
    
    TripleScanner scanner = client.getScanner(getIndex(), getPattern(),
        split.getLowTriple(), split.getHighTriple());
    
    return new TripleReader(scanner);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    
    Triple.COLLATION index = getIndex();
    Triple pattern = getPattern();
    int aggregationLevel = getAggregationLevel();
    
    SegmentInfo[] segments = null == pattern ?
        client.getIndex(index) : client.getIndex(index, pattern);
        
    return getSplits(index, aggregationLevel, segments);
  }
  
  
  // extracted for testing.
  List<InputSplit> getSplits(Triple.COLLATION index, int aggregationLevel,
      SegmentInfo[] segments) {
    
    List<InputSplit> splits = new ArrayList<InputSplit>(segments.length);
    List<String> locations = new ArrayList<String>();
    Triple splitLow = null;
    Triple splitLowestKnown = null;
    for (int i=0; i<segments.length; ++i) {
      Triple segmentLow = aggregate(index, aggregationLevel, segments[i].getLowTriple());
      if (null == splitLow) {
        splitLow = splitLowestKnown = segmentLow;
        if (null != client) {
          locations.add(client.locateSegment(segments[i]).getHostName());
        }
      } else {
        if (3 > aggregationLevel && splitLowestKnown.isMagic()) {
          splitLowestKnown = segmentLow;
        }
        if (!splitLowestKnown.equals(segmentLow)) {
          // finish split
          splits.add(new IndexSplit(splitLow, segmentLow, 
              locations.toArray(new String[locations.size()])));
          locations.clear();
          if (null != client) {
            locations.add(client.locateSegment(segments[i]).getHostName());
          }
          splitLow = splitLowestKnown = segmentLow;
        } else {
          // include segment locating to this split
          if (null != client) {
            locations.add(client.locateSegment(segments[i]).getHostName());
          }
        }
      }
    }
    splits.add(new IndexSplit(splitLow, segments[segments.length-1].getHighTriple(), 
        locations.toArray(new String[locations.size()])));
    
    return splits;
  }
  
  
  private Triple.COLLATION getIndex() throws IOException {
    try {
      return Triple.COLLATION.parse(conf.get(INPUT_INDEX));
    } catch (IllegalArgumentException ex) {
      throw new IOException("Illegal input index", ex);
    }
  }

  
  private Triple getPattern() {
    String s = conf.get(PATTERN_SUBJ);
    String p = conf.get(PATTERN_PRED);
    String o = conf.get(PATTERN_OBJ);
    if (null == s && null == p && null == o) {
      return null;
    }
    return Triple.newPattern(s, p, o);
  }
  
  
  private int getAggregationLevel() {
    return conf.getInt(AGGREGATION_LEVEL, 3); // default is no aggregation
  }
  
  
  private Triple aggregate(Triple.COLLATION index, int level, Triple t) {
    if (t.isMagic()) {
      return t;
    }
    switch (level) {
      case 1: return index.mask(t, true);
      case 2: return index.mask(t, false);
      default: return t;
    }
  }

  

}
