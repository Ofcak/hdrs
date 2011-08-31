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

package de.hpi.fgis.hdrs;

public class LogFormatUtil {

  
  public static double MB(long bytes) {
    return bytes/(1024.0*1024.0);
  }
  
  public static double MB(int bytes) {
    return bytes/(1024.0*1024.0);
  }
  
  public static double MBperSec(long bytes, long millis) {
    double sec = millis/1000.0;
    double MB = MB(bytes);
    return 0 == sec ? 0 : MB/sec;
  }
  
  public static double MBperSec(int bytes, long millis) {
    double sec = millis/1000.0;
    double MB = MB(bytes);
    return 0 == sec ? 0 : MB/sec;
  }
  
}
