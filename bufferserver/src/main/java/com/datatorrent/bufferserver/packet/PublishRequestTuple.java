/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.packet;

import java.util.Arrays;

import com.celeral.netlet.util.Throwables;
import com.celeral.netlet.util.VarInt;

/**
 * <p>
 * PublishRequestTuple class.</p>
 *
 * @since 0.3.2
 */
public class PublishRequestTuple extends GenericRequestTuple
{
  protected int blockSize;

  public PublishRequestTuple(byte[] array, int offset, int len)
  {
    super(array, offset, len);
  }

  @Override
  public int parse()
  {
    int dataOffset = super.parse();
    if (isValid()) {
      try {
        blockSize = readVarInt(dataOffset, offset + length);
        while (buffer[dataOffset++] < 0) {
        }
      }
      catch (NumberFormatException ex) {
        Throwables.throwFormatted(ex, RuntimeException.class,
                                  "Unable to parse the blockSize!", ex);
      }
    }

    return dataOffset;
  }

  public static byte[] getSerializedRequest(final String version,
                                            final String identifier,
                                            final long startingWindowId,
                                            final int blockSize)
  {
    byte[] bytes = new byte[4096];
    int offset = getSerializedRequest(bytes, 0, bytes.length,
                                      MessageType.PUBLISHER_REQUEST_VALUE, version, identifier, startingWindowId);

    /* write the blocksize at the end of the array */
    offset = VarInt.write(blockSize, bytes, offset);
    return Arrays.copyOfRange(bytes, 0, offset);
  }
  
  public int getBlockSize()
  {
    return blockSize;
  }
}
