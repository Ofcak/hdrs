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

package de.hpi.fgis.hdrs.tools;

import java.io.IOException;

import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;
import de.hpi.fgis.hdrs.triplefile.TripleFileUtils;

public class CopyTripleFile {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    
    System.out.print("copying... ");
    
    TripleFileInfo info = TripleFileUtils.copy(
        "C:\\hdrs\\infobox_properties_en.tf", 
        "C:\\hdrs\\infobox_properties_en2.tf", 
        1024 * 1024, Compression.ALGORITHM.DEFLATE);
    
    System.out.println("done. \n" + info);
  }
  
  
  
  
}
