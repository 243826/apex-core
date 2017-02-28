/*
 * Copyright 2017 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

/**
 *
 * @author Chetan Narsude  <chetan@apache.org>
 */
public class StreamingAppMasterServiceTest
{

  @Test
  public void testContainerSize()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.VCORES, 1);
    dag.setOperatorAttribute(o2, OperatorContext.VCORES, 2);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);
    dag.addStream("o2.outport1", o2.outport1, o3.inport1);

    dag.setOperatorAttribute(o2, OperatorContext.MEMORY_MB, 4000);
    dag.setAttribute(DAGContext.CONTAINERS_MAX_COUNT, 2);

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());
    Assert.assertEquals("memory container 1", 2560, plan.getContainers().get(0).getRequiredMemoryMB());
    Assert.assertEquals("vcores container 1", 1, plan.getContainers().get(0).getRequiredVCores());
    Assert.assertEquals("memory container 2", 4512, plan.getContainers().get(1).getRequiredMemoryMB());
    Assert.assertEquals("vcores container 2", 2, plan.getContainers().get(1).getRequiredVCores());
    Assert.assertEquals("number of operators in container 1", 2, plan.getContainers().get(0).getOperators().size());
  }

}
