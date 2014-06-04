/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;

public class IdBasedBlockDirectory {
  private File root;
  private static final String SEP = System.getProperty("file.separator");

  public IdBasedBlockDirectory(File root) throws IOException {
    if (!root.mkdirs() && !root.isDirectory()) {
      throw new IOException("Mkdirs failed to create " + root.toString());
    }
    this.root = root;
  }

  public File getRoot() {
    return root;
  }

  /**
   * Get the directory where a block with this ID should be stored and try to
   * create the directory if it doesn't exist.
   * @param blockId
   * @return
   * @throws IOException if directory creation fails
   */
  public File getDirectory(long blockId) throws IOException {
    File dir = getDirectoryNoCreate(this.root, blockId);
    if (!dir.mkdirs() && !dir.isDirectory()) {
      throw new IOException("Mkdirs failed to create " + dir.toString());
    }
    return dir;
  }

  /**
   * Get the directory where a block with this ID should be stored. Do not
   * attempt to create the directory.
   * @param root the root directory where blocks are stored
   * @param blockId
   * @return
   */
  public static File getDirectoryNoCreate(File root, long blockId) {
    int d1 = (int)((blockId >> 8) & 0xff);
    int d2 = (int)((blockId >> 16) & 0xff);
    StringBuilder sb = new StringBuilder();
    sb.append(DataStorage.BLOCK_SUBDIR_PREFIX + Integer.toHexString(d1)).
        append(SEP).
        append(DataStorage.BLOCK_SUBDIR_PREFIX + Integer.toHexString(d2));
    return new File(root, sb.toString());
  }
}