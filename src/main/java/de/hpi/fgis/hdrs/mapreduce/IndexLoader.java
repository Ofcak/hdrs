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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hpi.fgis.hdrs.Triple;

public class IndexLoader extends Configured implements Tool {
  
  public static class Map 
    extends Mapper<NullWritable, Triple, NullWritable, Triple> {
    
    public void map(NullWritable key, Triple value, Context context)
        throws IOException, InterruptedException {
      
      context.write(key, value);
    }
  }
  
  
  @Override
  public int run(String[] args) throws Exception {
    if (3 != args.length) {
      System.out.println("Usage: IndexLoader <StoreAddres> <SourceIndex> " +
      		"<TargetIndex1>[,<TargetIndex2>...]");
      return 0;
    }
    
    Job job = new Job(getConf());
    job.setJarByClass(IndexLoader.class);
    job.setJobName("HDRS Index Loader");
     
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(TripleOutputFormat.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(TripleOutputFormat.class);
         
    job.setMapperClass(Map.class);    
    job.setNumReduceTasks(0);
     
    job.setInputFormatClass(TripleInputFormat.class);
    job.setOutputFormatClass(TripleOutputFormat.class);
        
    TripleInputFormat.setStoreAddress(job, args[0]);
    TripleInputFormat.setIndex(job, args[1]);
    
    TripleOutputFormat.setStoreAddress(job, args[0]);
    TripleOutputFormat.setOutputIndexes(job, args[2]);
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IndexLoader(), args);
    System.exit(ret);
  }

}
