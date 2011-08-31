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

package de.hpi.fgis.hdrs.compression;

import java.io.IOException;
import java.util.zip.Deflater;

public class DeflateCompressor implements BlockCompressor {

  private final Deflater deflater = new Deflater(Deflater.BEST_SPEED);
  
  @Override
  public int compress(byte[] uncompressed, int uncompressedOffset,
      int uncompressedLength, byte[] compressed, int compressedOffset) throws IOException {
    writeInt(compressed, compressedOffset, uncompressedLength);
    deflater.setInput(uncompressed, uncompressedOffset, uncompressedLength);
    deflater.finish();
    int compressedLength = deflater.deflate(compressed, compressedOffset+4, uncompressedLength);
    deflater.reset();
    return compressedLength+4;
  }

  @Override
  public int maxCompressedLength(int byteSize) {
    return byteSize + 4;
  }
  
  public static void writeInt(byte[] buf, int off, int val) {
    buf[off] =   (byte) ((val >> 24) & 0xFF);
    buf[off+1] = (byte) ((val >> 16) & 0xFF);
    buf[off+2] = (byte) ((val >> 8) & 0xFF);
    buf[off+3] = (byte) ((val >> 0) & 0xFF);
  }

}
