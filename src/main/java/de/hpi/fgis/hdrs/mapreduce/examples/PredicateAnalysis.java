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

package de.hpi.fgis.hdrs.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.mapreduce.TripleInputFormat;

public class PredicateAnalysis extends Configured implements Tool {
  
  public static class Map 
    extends Mapper<NullWritable, Triple, BytesWritable, LongWritable> {
    
    private Triple current = null;
    private long count = 0;
    
    public void map(NullWritable key, Triple value, Context context)
        throws IOException, InterruptedException {
      
      if (null == current) {
        current = value;
      }
      
      if (Triple.buffersEqual(current.getBuffer(), 
          current.getObjectOffset(), current.getObjectLength(), 
          value.getBuffer(), 
          value.getObjectOffset(), value.getObjectLength())) {
        count += value.getMultiplicity();
      } else {
        BytesWritable object = new BytesWritable();
        object.set(current.getBuffer(), current.getObjectOffset(), current.getObjectLength());
        context.write(object, new LongWritable(count));
        
        current = value;
        count = 1;
      }
    }
  }
  
  public static class Reduce
    extends Reducer<BytesWritable, LongWritable, Text, Text> {
    
    public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context) 
    throws IOException, InterruptedException {
      context.write(new Text(new String(key.getBytes(), 0, key.getLength())), 
          new Text(""+values.iterator().next()));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf());
    job.setJarByClass(PredicateAnalysis.class);
    job.setJobName("Predicate Analysis");
     
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(LongWritable.class);
         
    job.setMapperClass(Map.class);
    //job.setReducerClass(Reduce.class);
    
    job.setNumReduceTasks(0);
     
    job.setInputFormatClass(TripleInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
    TripleInputFormat.setStoreAddress(job, args[0]);
    TripleInputFormat.setIndex(job, "POS");
    TripleInputFormat.setPattern(job, Triple.newPattern(null, args[1], null));
    TripleInputFormat.setAggregationLevel2(job);
    
    SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new PredicateAnalysis(), args);
    System.exit(ret);
  }

}
