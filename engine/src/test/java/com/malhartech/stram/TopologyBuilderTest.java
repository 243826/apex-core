/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.NewTopologyBuilder.StreamBuilder;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.InputPort;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.Topology.StreamDecl;
import com.malhartech.stram.conf.TopologyBuilder;

public class TopologyBuilderTest {

  public static NodeDecl assertNode(Topology tplg, String id) {
      NodeDecl n = tplg.getNode(id);
      assertNotNull("node exists id=" + id, n);
      return n;
  }

  /**
   * Test read from stram-site.xml in Hadoop configuration format.
   */
  @Test
  public void testLoadFromConfigXml() {
    Configuration conf = TopologyBuilder.addStramResources(new Configuration());
    //Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

    TopologyBuilder tb = new TopologyBuilder(conf);
    Topology tplg = tb.getTopology();
    tplg.validate();

//    Map<String, NodeConf> nodeConfs = tb.getAllNodes();
    assertEquals("number of node confs", 6, tplg.getAllNodes().size());

    NodeDecl node1 = assertNode(tplg, "node1");
    NodeDecl node2 = assertNode(tplg, "node2");
    NodeDecl node3 = assertNode(tplg, "node3");
    NodeDecl node4 = assertNode(tplg, "node4");

    assertNotNull("nodeConf for root", node1);
    assertEquals("nodeId set", "node1", node1.getId());

    // verify node instantiation
    AbstractNode dNode = initNode(node1);
    assertNotNull(dNode);
    assertEquals(dNode.getClass(), EchoNode.class);
    EchoNode echoNode = (EchoNode)dNode;
    assertEquals("myStringPropertyValue", echoNode.getMyStringProperty());

    // check links
    assertEquals("node1 inputs", 0, node1.getInputStreams().size());
    assertEquals("node1 outputs", 1, node1.getOutputStreams().size());
    StreamDecl n1n2 = node2.getInputStreams().get(EchoNode.INPUT1);
    assertNotNull("n1n2", n1n2);

    // output/input stream object same
    assertEquals("rootNode out is node2 in", n1n2, node1.getOutputStreams().get(EchoNode.OUTPUT1));
    assertEquals("n1n2 source", node1, n1n2.getSource().getNode());
    Assert.assertEquals("n1n2 targets", 1, n1n2.getSinks().size());
    Assert.assertEquals("n1n2 target", node2, n1n2.getSinks().get(0).getNode());

    assertEquals("stream name", "n1n2", n1n2.getId());
    Assert.assertFalse("n1n2 not inline (default)", n1n2.isInline());

    // node 2 streams to node 3 and node 4
    assertEquals("node 2 number of outputs", 1, node2.getOutputStreams().size());
    StreamDecl fromNode2 = node2.getOutputStreams().values().iterator().next();

    Set<NodeDecl> targetNodes = new HashSet<NodeDecl>();
    for (InputPort ip : fromNode2.getSinks()) {
      targetNodes.add(ip.getNode());
    }
    Assert.assertEquals("outputs " + fromNode2, Sets.newHashSet(node3, node4), targetNodes);

    NodeDecl node6 = assertNode(tplg, "node6");

    List<NodeDecl> rootNodes = tplg.getRootNodes();
    assertEquals("number root nodes", 2, rootNodes.size());
    assertTrue("root node2", rootNodes.contains(node1));
    assertTrue("root node6", rootNodes.contains(node6));

    for (NodeDecl n : rootNodes) {
      printTopology(n, tplg, 0);
    }

  }

  @SuppressWarnings("unchecked")
  private <T extends AbstractNode> T initNode(NodeDecl nodeConf) {
    return (T)StramUtils.initNode(nodeConf.getNodeClass(), nodeConf.getProperties());
  }

  public void printTopology(NodeDecl node, Topology tplg, int level) {
      String prefix = "";
      if (level > 0) {
        prefix = StringUtils.repeat(" ", 20*(level-1)) + "   |" + StringUtils.repeat("-", 17);
      }
      System.out.println(prefix + node.getId());
      for (StreamDecl downStream : node.getOutputStreams().values()) {
          if (!downStream.getSinks().isEmpty()) {
            for (InputPort targetNode : downStream.getSinks()) {
              printTopology(targetNode.getNode(), tplg, level+1);
            }
          }
      }
  }

  @Test
  public void testLoadFromPropertiesFile() throws IOException {
      Properties props = new Properties();
      String resourcePath = "/testTopology.properties";
      InputStream is = this.getClass().getResourceAsStream(resourcePath);
      if (is == null) {
        fail("Could not load " + resourcePath);
      }
      props.load(is);
      TopologyBuilder pb = new TopologyBuilder()
        .addFromProperties(props);

      Topology tplg = pb.getTopology();
      tplg.validate();

      assertEquals("number of node confs", 5, tplg.getAllNodes().size());
      assertEquals("number of root nodes", 3, tplg.getRootNodes().size());

      StreamDecl s1 = tplg.getStream("n1n2");
      assertNotNull(s1);
      assertTrue("n1n2 inline", s1.isInline());

      NodeDecl node3 = tplg.getNode("node3");
      Map<String, String> node3Props = node3.getProperties();

      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", node3Props.get("myStringProperty"));
      assertEquals("node3.classname", EchoNode.class.getName(), node3Props.get(TopologyBuilder.NODE_CLASSNAME));

      EchoNode dnode3 = initNode(node3);
      assertEquals("node3.myStringProperty", "myStringPropertyValueFromTemplate", dnode3.myStringProperty);
      assertFalse("node3.booleanProperty", dnode3.booleanProperty);

      NodeDecl node4 = tplg.getNode("node4");
      assertEquals("node4.myStringProperty", "overrideNode4", node4.getProperties().get("myStringProperty"));
      EchoNode dnode4 = (EchoNode)initNode(node4);
      assertEquals("node4.myStringProperty", "overrideNode4", dnode4.myStringProperty);
      assertTrue("node4.booleanProperty", dnode4.booleanProperty);

      StreamDecl input1 = tplg.getStream("inputToNode1");
      assertNotNull(input1);
      Assert.assertEquals("input1 source", tplg.getNode("inputNode"), input1.getSource().getNode());
      Assert.assertEquals("input1 target ", tplg.getNode("node1"), input1.getSinks().iterator().next().getNode());

  }

  @Test
  public void testCycleDetection() {
     NewTopologyBuilder b = new NewTopologyBuilder();

     //NodeConf node1 = b.getOrAddNode("node1");
     NodeDecl node2 = b.addNode("node2", EchoNode.class);
     NodeDecl node3 = b.addNode("node3", EchoNode.class);
     NodeDecl node4 = b.addNode("node4", EchoNode.class);
     //NodeConf node5 = b.getOrAddNode("node5");
     //NodeConf node6 = b.getOrAddNode("node6");
     NodeDecl node7 = b.addNode("node7", EchoNode.class);

     // strongly connect n2-n3-n4-n2
     b.addStream("n2n3")
       .setSource(node2.getOutput(EchoNode.OUTPUT1))
       .addSink(node3.getInput(EchoNode.INPUT1));

     b.addStream("n3n4")
       .setSource(node3.getOutput(EchoNode.OUTPUT1))
       .addSink(node4.getInput(EchoNode.INPUT1));

     b.addStream("n4n2")
       .setSource(node4.getOutput(EchoNode.OUTPUT1))
       .addSink(node2.getInput(EchoNode.INPUT1));

     // self referencing node cycle
     StreamBuilder n7n7 = b.addStream("n7n7")
         .setSource(node7.getOutput(EchoNode.OUTPUT1))
         .addSink(node7.getInput(EchoNode.INPUT1));
     try {
       n7n7.addSink(node7.getInput(EchoNode.INPUT1));
       fail("cannot add to stream again");
     } catch (Exception e) {
       // expected, stream can have single input/output only
     }

     Topology tplg = b.getTopology();

     List<List<String>> cycles = new ArrayList<List<String>>();
     tplg.findStronglyConnected(node7, cycles);
     assertEquals("node self reference", 1, cycles.size());
     assertEquals("node self reference", 1, cycles.get(0).size());
     assertEquals("node self reference", node7.getId(), cycles.get(0).get(0));

     // 3 node cycle
     cycles.clear();
     tplg.findStronglyConnected(node4, cycles);
     assertEquals("3 node cycle", 1, cycles.size());
     assertEquals("3 node cycle", 3, cycles.get(0).size());
     assertTrue("node2", cycles.get(0).contains(node2.getId()));
     assertTrue("node3", cycles.get(0).contains(node3.getId()));
     assertTrue("node4", cycles.get(0).contains(node4.getId()));

     try {
       tplg.validate();
       fail("validation should fail");
     } catch (IllegalStateException e) {
       // expected
     }

  }

  public static class TestSerDe extends DefaultSerDe {

  }

  /**
   * Node for topology testing.
   * Test should reference the ports defined using the constants.
   */
  @NodeAnnotation(
      ports = {
          @PortAnnotation(name = EchoNode.INPUT1,  type = PortType.INPUT),
          @PortAnnotation(name = EchoNode.INPUT2,  type = PortType.INPUT),
          @PortAnnotation(name = EchoNode.OUTPUT1, type = PortType.OUTPUT)
      }
  )
  public static class EchoNode extends AbstractNode {
    public static final String INPUT1 = "input1";
    public static final String INPUT2 = "input2";
    public static final String OUTPUT1 = "output1";

    private static final Logger logger = LoggerFactory.getLogger(EchoNode.class);

    boolean booleanProperty;

    private String myStringProperty;

    public String getMyStringProperty() {
      return myStringProperty;
    }

    public void setMyStringProperty(String myStringProperty) {
      this.myStringProperty = myStringProperty;
    }

    public boolean isBooleanProperty() {
      return booleanProperty;
    }

    public void setBooleanProperty(boolean booleanProperty) {
      this.booleanProperty = booleanProperty;
    }

    @Override
    public void process(Object o) {
      logger.info("Got some work: " + o);
    }

    @Override
    public void handleIdleTimeout()
    {
      deactivate();
    }
  }

}
