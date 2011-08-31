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
import java.io.DataOutputStream;
import java.io.IOException;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;

/**
 * Basically a fixed length struct that stores basic information
 * and important offsets for the file.
 * 
 * @author hefenbrock
 *
 */
class FileTrailer {
  
  Triple.COLLATION order;
  Compression.ALGORITHM compression;
  long indexOffset;
  // uncompressed index size
  int indexSize;
  // how many blocks in file? (equals number of index entries)
  int blockCount;
  // total uncompressed size of this file
  long uncompressedSize;
  // size of all triples in this file
  long dataSize;
  //
  long nTriples;
  
  // update this!
  public static final int SIZE  = (
      2*Byte.SIZE // for order, compression 
      + 2*Integer.SIZE
      + 4*Long.SIZE
      ) / Byte.SIZE;
  
  public void write(DataOutputStream stream) throws IOException {
    stream.writeByte(order.getCode());
    stream.writeByte(compression.getCode());
    stream.writeLong(indexOffset);
    stream.writeInt(indexSize);
    stream.writeInt(blockCount);
    stream.writeLong(uncompressedSize);
    stream.writeLong(dataSize);
    stream.writeLong(nTriples);
  }
  
  public void read(DataInput in) throws IOException {
    order = Triple.COLLATION.decode(in.readByte());
    compression = Compression.ALGORITHM.decode(in.readByte());
    indexOffset = in.readLong();
    indexSize = in.readInt();
    blockCount = in.readInt();
    uncompressedSize = in.readLong();
    dataSize = in.readLong();
    nTriples = in.readLong();
  }
  
  @Override
  public String toString() {
    return "FileTrailer(order = " + order
      + ", compression = " + compression
      + ", indexOffset = " + indexOffset
      + ", indexSize = " + indexSize
      + ", blockCount = " + blockCount 
      + ", uncompressedSize = " + uncompressedSize + ")";
  }
  
}
