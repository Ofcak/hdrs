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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import de.hpi.fgis.hdrs.Triple;

public class IndexSplit extends InputSplit implements Writable {

  private String[] locations;
  private Triple lowTriple;
  private Triple highTriple;
  
  
  public IndexSplit() {}
  
  
  IndexSplit(Triple lowTriple, Triple highTriple, String...locations) {
    this.locations = locations;
    this.lowTriple = lowTriple;
    this.highTriple = highTriple;
  }
  
  
  public Triple getLowTriple() {
    return lowTriple;
  }
  
  
  public Triple getHighTriple() {
    return highTriple;
  }
  
  
  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }
  

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return locations;
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    locations = new String[in.readInt()];
    for (int i=0; i<locations.length; ++i) {
      locations[i] = in.readLine();
    }
    lowTriple = Triple.readTriple(in);
    highTriple = Triple.readTriple(in);
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(locations.length);
    for (String location : locations) {
      out.writeBytes(location);
      out.write('\n');
    }
    lowTriple.write(out);
    highTriple.write(out);
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((highTriple == null) ? 0 : highTriple.hashCode());
    result = prime * result + ((lowTriple == null) ? 0 : lowTriple.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    IndexSplit other = (IndexSplit) obj;
    if (highTriple == null) {
      if (other.highTriple != null) {
        return false;
      }
    } else if (!highTriple.equals(other.highTriple)) {
      return false;
    }
    if (lowTriple == null) {
      if (other.lowTriple != null) {
        return false;
      }
    } else if (!lowTriple.equals(other.lowTriple)) {
      return false;
    }
    return true;
  }


  @Override
  public String toString() {
    return "IndexSplit [lowTriple=" + lowTriple
        + ", highTriple=" + highTriple + "]";
  }


  

}
