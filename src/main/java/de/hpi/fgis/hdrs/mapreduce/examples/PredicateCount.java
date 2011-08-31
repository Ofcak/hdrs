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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.mapreduce.TripleInputFormat;

public class PredicateCount extends Configured implements Tool {

  public static class Map 
    extends Mapper<NullWritable, Triple, BytesWritable, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private BytesWritable pred = new BytesWritable();
   
    public void map(NullWritable key, Triple value, Context context)
    throws IOException, InterruptedException {
      pred.set(value.getBuffer(), value.getPredicateOffset(), value.getPredicateLength());
      context.write(pred, one); 
    }
  }
  
  public static class Reduce
    extends Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable> {
    
    public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) 
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf());
    job.setJarByClass(PredicateCount.class);
    job.setJobName("PredicateCount");
     
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(IntWritable.class);
     
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
     
    job.setInputFormatClass(TripleInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    int argc = 0;
    
    TripleInputFormat.setStoreAddress(job, args[argc++]);
    TripleInputFormat.setIndex(job, args[argc++]);
    if ("-p".equals(args[argc])) {
      argc++;
      String s = args[argc++];
      String p = args[argc++];
      String o = args[argc++];
      if ("*".equals(s)) s = null;
      if ("*".equals(p)) p = null;
      if ("*".equals(o)) o = null;
      TripleInputFormat.setPattern(job, Triple.newPattern(s, p, o));
    } else {
      FileOutputFormat.setOutputPath(job, new Path(args[argc]));
    }
     
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new PredicateCount(), args);
    System.exit(ret);
  }

}
