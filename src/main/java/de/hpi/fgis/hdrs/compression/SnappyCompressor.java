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

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyException;

public class SnappyCompressor implements BlockCompressor {

  @Override
  public int compress(byte[] uncompressed, int uncompressedOffset,
      int uncompressedLen, byte[] compressed, int compressedOff)
      throws IOException {
    int compressedLength;
    try {
      compressedLength = Snappy.compress(uncompressed, uncompressedOffset, uncompressedLen, 
          compressed, compressedOff);
    } catch (SnappyException e) {
      throw new IOException("compression error", e);
    }
    return compressedLength;
  }

  @Override
  public int maxCompressedLength(int byteSize) {
    return Snappy.maxCompressedLength(byteSize);
  }

}
