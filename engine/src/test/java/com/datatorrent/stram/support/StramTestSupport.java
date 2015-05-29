/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.support;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import org.apache.commons.io.FileUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StorageAgent;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramLocalCluster.LocalStreamingContainer;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport
{
  private static final Logger LOG = LoggerFactory.getLogger(StramTestSupport.class);
  public static final long DEFAULT_TIMEOUT_MILLIS = 30000;

  public static Object generateTuple(Object payload, int windowId)
  {
    return payload;
  }

  public static Tuple generateBeginWindowTuple(String nodeid, int windowId)
  {
    Tuple bwt = new Tuple(MessageType.BEGIN_WINDOW, windowId);
    return bwt;
  }

  public static Tuple generateEndWindowTuple(String nodeid, int windowId)
  {
    EndWindowTuple t = new EndWindowTuple(windowId);
    return t;
  }

  public static void checkStringMatch(String print, String expected, String got)
  {
    assertTrue(
            print + " doesn't match, got: " + got + " expected: " + expected,
            got.matches(expected));
  }

  public static WindowGenerator setupWindowGenerator(ManualScheduledExecutorService mses)
  {
    WindowGenerator gen = new WindowGenerator(mses, 1024);
    gen.setResetWindow(0);
    gen.setFirstWindow(0);
    gen.setWindowWidth(1);
    return gen;
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static void waitForWindowComplete(OperatorContext nodeCtx, long windowId) throws InterruptedException
  {
    LOG.debug("Waiting for end of window {} at node {} when lastProcessedWindowId is {}", new Object[] {windowId, nodeCtx.getId(), nodeCtx.getLastProcessedWindowId()});
    long startMillis = System.currentTimeMillis();
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      if (System.currentTimeMillis() > (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
        long timeout = System.currentTimeMillis() - startMillis;
        throw new AssertionError(String.format("Timeout %s ms waiting for window %s operator %s", timeout, windowId, nodeCtx.getId()));
      }
      Thread.sleep(20);
    }
  }

  /**
   * Create an appPackage zip using the sample appPackage located in
   * src/test/resources/testAppPackage/testAppPackageSrc.
   * @param file  The file whose path will be used to create the appPackage zip
   * @return      The File object that can be used in the AppPackage constructor.
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public static File createAppPackageFile(File file) throws net.lingala.zip4j.exception.ZipException {
    ZipFile zipFile = new ZipFile(file);
    ZipParameters zipParameters = new ZipParameters();
    zipParameters.setIncludeRootFolder(false);
    zipFile.createZipFileFromFolder("src/test/resources/testAppPackage/testAppPackageSrc", zipParameters, false, Long.MAX_VALUE);
    return file;
  }

  /**
   * Create an confPackage zip using the sample confPackage located in
   * src/test/resources/testConfPackage/testConfPackageSrc.
   *
   * @param file The file whose path will be used to create the confPackage zip
   * @return The File object that can be used in the ConfigPackage constructor.
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public static File createConfigPackageFile(File file) throws net.lingala.zip4j.exception.ZipException
  {
    ZipFile zipFile = new ZipFile(file);
    ZipParameters zipParameters = new ZipParameters();
    zipParameters.setIncludeRootFolder(false);
    zipFile.createZipFileFromFolder("src/test/resources/testConfigPackage/testConfigPackageSrc", zipParameters, false, Long.MAX_VALUE);
    return file;
  }

  public interface WaitCondition
  {
    boolean isComplete();

  }

  @SuppressWarnings("SleepWhileInLoop")
  public static boolean awaitCompletion(WaitCondition c, long timeoutMillis) throws InterruptedException
  {
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + timeoutMillis)) {
      if (c.isComplete()) {
        return true;
      }
      sleep(50);
    }
    return c.isComplete();
  }

  /**
   * Wait until instance of operator is deployed into a container and return the container reference.
   * Asserts non null return value.
   *
   * @param localCluster
   * @param operator
   * @return
   * @throws InterruptedException
   */
  @SuppressWarnings("SleepWhileInLoop")
  public static LocalStreamingContainer waitForActivation(StramLocalCluster localCluster, PTOperator operator) throws InterruptedException
  {
    LocalStreamingContainer container;
    long startMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() < (startMillis + DEFAULT_TIMEOUT_MILLIS)) {
      if (operator.getState() == PTOperator.State.ACTIVE) {
        if ((container = localCluster.getContainer(operator)) != null) {
          return container;
        }
      }
      LOG.debug("Waiting for {}({}) in container {}", new Object[] {operator, operator.getState(), operator.getContainer()});
      Thread.sleep(500);
    }

    Assert.fail("timeout waiting for operator deployment " + operator);
    return null;
  }

  public static class RegexMatcher extends BaseMatcher<String>
  {
    private final String regex;

    public RegexMatcher(String regex)
    {
      this.regex = regex;
    }

    @Override
    public boolean matches(Object o)
    {
      return ((String)o).matches(regex);

    }

    @Override
    public void describeTo(Description description)
    {
      description.appendText("matches regex=" + regex);
    }

    public static RegexMatcher matches(String regex)
    {
      return new RegexMatcher(regex);
    }

  }

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      //className = className.substring(className.lastIndexOf('.') + 1);
      this.dir = "target/" + className + "/" + methodName;
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(this.dir));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class TestHomeDirectory extends TestWatcher
  {

    Map<String, String> env = new HashMap<String, String>();
    String userHome;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      super.starting(description);
      try {
        userHome = System.getProperty("user.home");
        env.put("HOME", System.getProperty("user.dir") + "/src/test/resources/testAppPackage");
        setEnv(env);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      super.finished(description);

      try {
        env.put("HOME", userHome);
        setEnv(env);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void setEnv(Map<String, String> newenv) throws Exception
  {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>)theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>)theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>)obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }


  public static class MemoryStorageAgent implements StorageAgent, Serializable
  {
    static class OperatorWindowIdPair implements Serializable
    {
      final int operatorId;
      final long windowId;

      OperatorWindowIdPair(int operatorId, long windowId)
      {
        this.operatorId = operatorId;
        this.windowId = windowId;
      }

      @Override
      public int hashCode()
      {
        int hash = 7;
        hash = 97 * hash + this.operatorId;
        hash = 97 * hash + (int)(this.windowId ^ (this.windowId >>> 32));
        return hash;
      }

      @Override
      public boolean equals(Object obj)
      {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final OperatorWindowIdPair other = (OperatorWindowIdPair)obj;
        if (this.operatorId != other.operatorId) {
          return false;
        }
        if (this.windowId != other.windowId) {
          return false;
        }
        return true;
      }

      private static final long serialVersionUID = 201404091805L;
    }

    transient HashMap<OperatorWindowIdPair, Object> store = new HashMap<OperatorWindowIdPair, Object>();

    @Override
    public synchronized void save(Object object, int operatorId, long windowId) throws IOException
    {
      store.put(new OperatorWindowIdPair(operatorId, windowId), object);
    }

    @Override
    public synchronized Object load(int operatorId, long windowId) throws IOException
    {
      return store.get(new OperatorWindowIdPair(operatorId, windowId));
    }

    @Override
    public synchronized void delete(int operatorId, long windowId) throws IOException
    {
      store.remove(new OperatorWindowIdPair(operatorId, windowId));
    }

    @Override
    public synchronized long[] getWindowIds(int operatorId) throws IOException
    {
      ArrayList<Long> windowIds = new ArrayList<Long>();
      for (OperatorWindowIdPair key : store.keySet()) {
        if (key.operatorId == operatorId) {
          windowIds.add(key.windowId);
        }
      }

      long[] ret = new long[windowIds.size()];
      for (int i = ret.length; i-- > 0;) {
        ret[i] = windowIds.get(i).longValue();
      }

      return ret;
    }

    private static final long serialVersionUID = 201404091747L;
  }

}
