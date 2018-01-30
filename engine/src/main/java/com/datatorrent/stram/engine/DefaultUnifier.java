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
package com.datatorrent.stram.engine;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.Stateless;

/**
 * Default unifier passes through all tuples received. Used when an operator has
 * multiple partitions and no unifier was provided through
 *
 * @since 0.3.2
 */
@Stateless
public class DefaultUnifier implements Unifier<Object>, Serializable
{
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<>();

  @Override
  public void process(Object tuple)
  {
    long t = System.currentTimeMillis();
    try {
      outputPort.emit(tuple);
    }
    catch (Throwable th) {
      logger.error("Unifier emit failed on {}", tuple, th);
      throw com.celeral.netlet.util.DTThrowable.wrapIfChecked(th);
    }
    finally {
      t = System.currentTimeMillis() - t;
      if (t > 1000L) {
        logger.debug("Unifier emit took {}ms for {}", t, tuple);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    logger.trace("window begun!");
  }

  @Override
  public void endWindow()
  {
    logger.trace("window ended!");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    logger.debug("setup done!");
  }

  @Override
  public void teardown()
  {
    logger.debug("Operator torn down");
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(DefaultUnifier.class);
  private static final long serialVersionUID = 201404141917L;
}
