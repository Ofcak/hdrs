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

package de.hpi.fgis.hdrs.triplefile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.hpi.fgis.hdrs.LogFormatUtil;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.compression.Compression.ALGORITHM;
import de.hpi.fgis.hdrs.ipc.Writable;

/**
 * this struct-like class holds details about a specific triple file.
 * 
 * @author daniel.hefenbrock
 *
 */
public class TripleFileInfo implements Writable {

  private Triple.COLLATION order;
  private Compression.ALGORITHM compression;
  private boolean isHalfFile;
  private long size;
  private long uncompressedSize;
  private long uncompressedDataSize;
  private long nTriples;
  private String absolutePath;
  private int indexSize;
  
  
  public TripleFileInfo() {}
  
  
  public TripleFileInfo(COLLATION order, ALGORITHM compression,
      boolean isHalfFile, long size, long uncompressedSize, long uncompressedDataSize, 
      long nTriples, String absolutePath, int indexSize) {
    this.order = order;
    this.compression = compression;
    this.isHalfFile = isHalfFile;
    this.size = size;
    this.uncompressedSize = uncompressedSize;
    this.nTriples = nTriples;
    this.uncompressedDataSize = uncompressedDataSize;
    this.absolutePath = absolutePath;
    this.indexSize = indexSize;
  }
  
  
  TripleFileInfo(FileTrailer trailer, String absolutePath, long size) {
    this.order = trailer.order;
    this.compression = trailer.compression;
    this.isHalfFile = false;
    this.size = size;
    this.uncompressedSize = trailer.uncompressedSize;
    this.uncompressedDataSize = trailer.dataSize;
    this.nTriples = trailer.nTriples;
    this.absolutePath = absolutePath;
    this.indexSize = trailer.indexSize;
  }
  
  
  public Triple.COLLATION getOrder() {
    return order;
  }
  
  public Compression.ALGORITHM getCompression() {
    return compression;
  }
  
  public boolean isHalfFile() {
    return isHalfFile;
  }
  
  public long getSize() {
    return size;
  }
  
  public long getUncompressedSize() {
    return uncompressedSize;
  }
  
  public long getUncompressedDataSize() {
    return uncompressedDataSize;
  }
  
  public long getNumberOfTriples() {
    return nTriples;
  }
  
  public String getAbsolutePath() {
    return absolutePath;
  }
  
  public int getIndexSize() {
    return indexSize;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    order = Triple.COLLATION.decode(in.readByte());
    compression = Compression.ALGORITHM.decode(in.readByte());
    isHalfFile = in.readBoolean();
    size = in.readLong();
    uncompressedSize = in.readLong();
    uncompressedDataSize = in.readLong();
    nTriples = in.readLong();
    absolutePath = in.readLine();
    indexSize = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(order.getCode());
    out.writeByte(compression.getCode());
    out.writeBoolean(isHalfFile);
    out.writeLong(size);
    out.writeLong(uncompressedSize);
    out.writeLong(uncompressedDataSize);
    out.writeLong(nTriples);
    out.writeBytes(absolutePath);
    out.writeByte('\n');
    out.writeInt(indexSize);
  }
  
  @Override
  public String toString() {
    return "TripleFile(path = " + getAbsolutePath()
        + ", compression = " + getCompression()
        + ", order = " + getOrder()
        + ", length = " + LogFormatUtil.MB(getSize())
        + ", lengh (uncompressed) = " + LogFormatUtil.MB(getUncompressedSize())
        + ", data size (uncompressed) = " + LogFormatUtil.MB(getUncompressedDataSize())
        + ", number of triples = " + getNumberOfTriples()
        + ", index size = " + LogFormatUtil.MB(getIndexSize())
        + ")";
  }
  
}
