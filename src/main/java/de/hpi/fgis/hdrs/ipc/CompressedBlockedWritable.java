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

public abstract class CompressedBlockedWritable implements Writable {

  abstract void read(BlockCompressorInput in) throws IOException;
  
  abstract void write(BlockCompressorOutput out) throws IOException;
  
  @Override
  public void readFields(DataInput in) throws IOException {
    
    BlockCompressorInput blockInput = new BlockCompressorInput(in);
    read(blockInput);
    
  }

  @Override
  public void write(DataOutput out) throws IOException {

    BlockCompressorOutput blockOutput = new BlockCompressorOutput(out);
    write(blockOutput);
    blockOutput.close();
    
  }
  
  
  
  static class BlockCompressorInput {
    
    final DataInput in;
    final BlockDecompressor decompressor = Compression.ALGORITHM.SNAPPY.getDecompressor();
    
    protected BlockCompressorInput(DataInput in) {
      this.in = in;
    }
    
    ByteBuffer read() throws IOException {
      int compressedSize = in.readInt();
      
      // block or over?
      if (0 >= compressedSize) {
        return null;
      }
      
      byte[] compressed = new byte[compressedSize];
      in.readFully(compressed);
    
      int uncompressedSize = decompressor.uncompressedLength(compressed, 0, compressedSize);
      byte[] uncompressed = new byte[uncompressedSize];
    
      decompressor.uncompress(compressed, 0, compressedSize, uncompressed, 0);
    
      return ByteBuffer.wrap(uncompressed);
    }
  }
  
  
  static class BlockCompressorOutput {
    
    final DataOutput out;
    final BlockCompressor compressor = Compression.ALGORITHM.SNAPPY.getCompressor();
    
    protected  BlockCompressorOutput(DataOutput out) {
      this.out = out;
    }
    
    void write(ByteBuffer block) throws IOException {
      block.rewind();
      
      byte[] compressed = new byte[compressor.maxCompressedLength(block.limit())];
      
      int compressedSize = compressor.compress(block.array(), block.arrayOffset(), 
          block.limit(), compressed, 0);
      
      out.writeInt(compressedSize);
      out.write(compressed, 0, compressedSize);
    }
    
    protected void close() throws IOException {
      out.writeInt(-1);
    }
    
  }

}
