/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.policy;

import java.util.Set;

import com.datatorrent.bufferserver.internal.PhysicalNode;
import com.datatorrent.bufferserver.util.SerializedData;

/**
 *
 * The base class for specifying partition policies, implements interface {@link com.datatorrent.bufferserver.policy.Policy}<p>
 * <br>
 *
 * @author chetan
 * @since 0.3.2
 */
public class AbstractPolicy implements Policy
{

  /**
   *
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   */

  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
