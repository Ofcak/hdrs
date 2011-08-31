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
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.client.TripleOutputStream;

public class TripleOutputFormat extends OutputFormat<NullWritable, Triple> 
implements Configurable {
  
  public static final String OUTPUT_INDEXES = "hdrs.mapreduce.outputindexes";
  
  
  private Configuration conf;
  
  @Override
  public void setConf(org.apache.hadoop.conf.Configuration conf) {
    this.conf = new Configuration(conf);
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public static void setStoreAddress(Job job, String address) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.set(TripleInputFormat.STORE_ADDRESS, address);
  }
  
  /**
   * <p>Set output indexes to write.  Takes a comma separated string of
   * indexes, eg. "SPO,POS.OSP".
   * 
   * <p>If this option is not set, all indexes present in the store 
   * will be written.
   */
  public static void setOutputIndexes(Job job, String indexes) {
    org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
    conf.set(OUTPUT_INDEXES, indexes);
  }
  
  
  private static Set<Triple.COLLATION> getOutputIndexes(Configuration conf) {
    String str = conf.get(OUTPUT_INDEXES);
    if (null == str) {
      return null;
    }
    Set<Triple.COLLATION> indexes = EnumSet.noneOf(Triple.COLLATION.class);
    for (String index : str.split(",")) {
      indexes.add(Triple.COLLATION.parse(index));
    }
    return indexes;
  }
  
  
  private static class TripleWriter extends RecordWriter<NullWritable, Triple> {

    private final Client client;
    private final TripleOutputStream out;
    
    TripleWriter(Client client, TripleOutputStream out) {
      this.client = client;
      this.out = out;
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      out.close();
      client.close();
    }

    @Override
    public void write(NullWritable key, Triple value) throws IOException,
        InterruptedException {
      out.add(value);
    }
    
  }
  
  
  private static class TripleOutputCommitter extends OutputCommitter {

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {      
    }

    @Override
    public void cleanupJob(JobContext jobContext) throws IOException {      
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {      
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {      
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {      
    }
    
  }
  
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    
    Client client = new Client(this.conf, conf.get(TripleInputFormat.STORE_ADDRESS));
    
    if (null != conf.get(TripleInputFormat.STORE_ADDRESS)) {
      for (Triple.COLLATION index : getOutputIndexes(conf)) {
        if (!client.getIndexes().contains(index)) {
          throw new IOException("HDRS store @ " + conf.get(TripleInputFormat.STORE_ADDRESS) 
              + " does not contain index " + index);
        }
      }
    }
    
    client.close();
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TripleOutputCommitter();
  }

  @Override
  public RecordWriter<NullWritable, Triple> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    
    Client client = new Client(this.conf, conf.get(TripleInputFormat.STORE_ADDRESS));
    
    return new TripleWriter(client, null==conf.get(OUTPUT_INDEXES) 
        ? client.getOutputStream()
        : client.getOutputStream(getOutputIndexes(conf)));
  }

  

}
