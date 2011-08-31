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


public class Compression {
  
  public enum ALGORITHM {
    
    NONE ((byte) 0) {

      @Override
      public BlockCompressor getCompressor() {
        throw new UnsupportedOperationException();
      }

      @Override
      public BlockDecompressor getDecompressor() {
        throw new UnsupportedOperationException();
      }
      
    },
    DEFLATE ((byte) 1) {

      @Override
      public BlockCompressor getCompressor() {
        return new DeflateCompressor();
      }

      @Override
      public BlockDecompressor getDecompressor() {
        return new DeflateDecompressor();
      }
      
    },
    SNAPPY ((byte) 2) {

      @Override
      public BlockCompressor getCompressor() {
        return new SnappyCompressor();
      }

      @Override
      public BlockDecompressor getDecompressor() {
        return new SnappyDecompressor();
      }
      
    };
    
    
    public abstract BlockCompressor getCompressor();
    public abstract BlockDecompressor getDecompressor();
    
    
    private final byte code;
    
    ALGORITHM(byte c) {
      code = c;
    }
    
    public byte getCode() {
      return code;
    }
    
    public static Compression.ALGORITHM decode(final byte code) {
      if (NONE.code == code) return NONE;
      if (DEFLATE.code == code) return DEFLATE;
      if (SNAPPY.code == code) return SNAPPY;
      throw new IllegalArgumentException("Invalid compression " +
      		" algoritm encoding");
    }
    
  }
  
  
  
}
