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
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateDecompressor implements BlockDecompressor {

  private final Inflater inflater = new Inflater();
  
  @Override
  public int uncompress(byte[] compressed, int compressedOffset,
      int compressedLength, byte[] uncompressed, int uncompressedOffset) throws IOException {
    inflater.setInput(compressed, compressedOffset+4, compressedLength-4);
    int unompressedLength;
    try {
      unompressedLength = inflater.inflate(uncompressed, uncompressedOffset, 
          readInt(compressed, compressedOffset));
    } catch (DataFormatException e) {
      throw new IOException("bad data format for decompression", e);
    }
    return unompressedLength;
  }

  @Override
  public int uncompressedLength(byte[] compressed, int offset, int length) {
    return readInt(compressed, offset);
  }
  
  public static int readInt(byte[] buffer, int off) {
    int b1 = (buffer[off] & 0xFF) << 24;
    int b2 = (buffer[off + 1] & 0xFF) << 16;
    int b3 = (buffer[off + 2] & 0xFF) << 8;
    int b4 = buffer[off + 3] & 0xFF;
    return b1 | b2 | b3 | b4;
  }
}
