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

import de.hpi.fgis.hdrs.Configuration;

public class SegmentConfiguration {

  private final int bufferSize;
  private final int flushThreshold;
  
  private final int splitSize;
  
  private final int minCompactSize;
  private final int maxBatchAdd;
  
  private final int maxFiles;
  private final int minCompactFiles;
  private final int maxCompactFiles;
  private final float compactionRatio;
  
  public static final int MAX_BATCH_ADD = 16*1024;
  
  public SegmentConfiguration(Configuration conf) {
    this(
    conf.getInt(
        Configuration.KEY_SEGMENT_BUFFER_SIZE, 
        Configuration.DEFAULT_SEGMENT_BUFFER_SIZE),
    conf.getInt(
        Configuration.KEY_SEGMENT_FLUSH_THRESHOLD, 
        Configuration.DEFAULT_SEGMENT_FLUSH_THRESHOLD),
    conf.getInt(
        Configuration.KEY_SEGMENT_SPLIT_SIZE, 
        Configuration.DEFAULT_SEGMENT_SPLIT_SIZE),
    conf.getInt(
        Configuration.KEY_SEGMENT_COMPACTION_THRESHOLD, 
        Configuration.DEFAULT_SEGMENT_COMPACTION_THRESHOLD),
    MAX_BATCH_ADD,
    conf.getInt(
        Configuration.KEY_SEGMENT_MAX_FILES, 
        Configuration.DEFAULT_SEGMENT_MAX_FILES),
    conf.getInt(
        Configuration.KEY_SEGMENT_MIN_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_MIN_COMPACTION),
    conf.getInt(
        Configuration.KEY_SEGMENT_MAX_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_MAX_COMPACTION),
    conf.getFloat(
        Configuration.KEY_SEGMENT_COMPACTION_RATIO,
        Configuration.DEFAULT_SEGMENT_COMPACTION_RATIO));
  }
  
  public SegmentConfiguration() {
    this(Configuration.DEFAULT_SEGMENT_BUFFER_SIZE, 
        Configuration.DEFAULT_SEGMENT_FLUSH_THRESHOLD,
        Configuration.DEFAULT_SEGMENT_SPLIT_SIZE, 
        Configuration.DEFAULT_SEGMENT_COMPACTION_THRESHOLD,
        MAX_BATCH_ADD,
        Configuration.DEFAULT_SEGMENT_MAX_FILES,
        Configuration.DEFAULT_SEGMENT_MIN_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_MAX_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_COMPACTION_RATIO);
  }
  
  public SegmentConfiguration(int bufferSize, int flushThreshold) {
    this(bufferSize, flushThreshold,
        Configuration.DEFAULT_SEGMENT_SPLIT_SIZE, 
        Configuration.DEFAULT_SEGMENT_COMPACTION_THRESHOLD,
        MAX_BATCH_ADD,
        Configuration.DEFAULT_SEGMENT_MAX_FILES,
        Configuration.DEFAULT_SEGMENT_MIN_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_MAX_COMPACTION, 
        Configuration.DEFAULT_SEGMENT_COMPACTION_RATIO);
  }
  
  public SegmentConfiguration(int bufferSize, int flushThreshold,
      int splitSize, int minCompactSize, int maxBatchAdd, int maxFiles,
      int minCompactFiles, int maxCompactFiles, float compactionRatio) {
    this.bufferSize = bufferSize;
    this.flushThreshold = flushThreshold;
    this.splitSize = splitSize;
    this.minCompactSize = minCompactSize;
    this.maxBatchAdd = maxBatchAdd;
    this.maxFiles = maxFiles;
    this.minCompactFiles = minCompactFiles;
    this.maxCompactFiles = maxCompactFiles;
    this.compactionRatio = compactionRatio;
  }
  
  
  /**
   * @return In-memory buffer size for segment.
   */
  public int getBufferSize() {
    return bufferSize;
  }
  
  /**
   * @return Flush threshold for segment.
   */
  public int getFlushThreshold() {
    return flushThreshold;
  }
  
  /**
   * @return Total on-disk(!) size at which the segment should be split.
   */
  public int getSplitThreshold() {
    return splitSize;
  }
  
  /**
   * @return Size at which to split in scatter mode.
   */
  public int getScatterThreshold() {
    return splitSize/8;
  }
  
  /**
   * @return Files smaller than this are always added to a minor compaction.
   */
  public int getMinCompactionSize() {
    return minCompactSize;
  }
  
  /**
   * @return Maximum amount of triples (in bytes) that can be added in one batch.
   */
  public int getMaxBatchadd() {
    return maxBatchAdd;
  }
  
  /**
   * @return Maximum number of files for segment.  Flushing will be stalled once
   * this number of files is reached.
   */
  public int getMaxFiles() {
    return maxFiles;
  }
  
  /**
   * @return Minimal number of files for a minor compaction.
   */
  public int getMinCompaction() {
    return minCompactFiles;
  }
  
  /**
   * @return Maximal number of files for a minor compaction.
   */
  public int getMaxCompaction() {
    return maxCompactFiles;
  }
  
  /**
   * @return Compaction ratio.
   */
  public float getCompactionRatio() {
    return compactionRatio;
  }

  @Override
  public String toString() {
    return "SegmentConfiguration("
    		+ "buffer size = " + getBufferSize()
    		+ ", flush threshold = " + getFlushThreshold()
    		+ ", max files = " + getMaxFiles()
    		+ ", split threshold = " + getSplitThreshold() 
    		+ ", scatter threshold = " + getScatterThreshold()
    		+ ", min compaction = " + getMinCompaction()
    		+ ", max compaction = " + getMaxCompaction() 
    		+ ", compaction threshold = " + getMinCompactionSize()
        + ", compaction ratio = " + getCompactionRatio()
        + ")";
  }
  
  
  

}
