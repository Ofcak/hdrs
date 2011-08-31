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

package de.hpi.fgis.hdrs.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.compression.BlockCompressor;
import de.hpi.fgis.hdrs.compression.BlockDecompressor;
import de.hpi.fgis.hdrs.compression.Compression;


public abstract class CompressedWritable implements Writable {
  
  public abstract void read(ByteBuffer in) throws IOException;
  
  public abstract ByteBuffer write() throws IOException;
  
  
  @Override
  public final void readFields(final DataInput in) throws IOException {
    int compressedSize = in.readInt();
    byte[] compressed = new byte[compressedSize];
    in.readFully(compressed);
    
    BlockDecompressor decompressor = Compression.ALGORITHM.SNAPPY.getDecompressor();
    
    int uncompressedSize = decompressor.uncompressedLength(compressed, 0, compressedSize);
    byte[] uncompressed = new byte[uncompressedSize];
    
    decompressor.uncompress(compressed, 0, compressedSize, uncompressed, 0);
    
    read(ByteBuffer.wrap(uncompressed));
  }

  
  
  @Override
  public final void write(final DataOutput out) throws IOException {
    
    ByteBuffer uncompressed = write();
    uncompressed.rewind();
    
    BlockCompressor compressor = Compression.ALGORITHM.SNAPPY.getCompressor();
    
    byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.limit())];
    
    int compressedSize = compressor.compress(uncompressed.array(), uncompressed.arrayOffset(), 
        uncompressed.limit(), compressed, 0);
    
    out.writeInt(compressedSize);
    out.write(compressed, 0, compressedSize);
  }
  
  
}
