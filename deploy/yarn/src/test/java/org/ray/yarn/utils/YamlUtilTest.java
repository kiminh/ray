package org.ray.yarn.utils;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class YamlUtilTest {

  @Test
  public void testGetProtocol() {
    assertEquals("http", YamlUtil.getProtocol("http://ray.org/t/ray.conf"));
    assertEquals("https", YamlUtil.getProtocol("https://ray.org/t/ray.conf"));
    assertEquals("ftp", YamlUtil.getProtocol("ftp://user:password@ray.org:2121/"));
    assertEquals("file", YamlUtil.getProtocol("file://ray/t/ray.conf"));
    assertEquals("file", YamlUtil.getProtocol("/ray/t/ray.conf"));
    assertEquals("file", YamlUtil.getProtocol("./ray/t/ray.conf"));
    assertEquals("file", YamlUtil.getProtocol("../ray/t/ray.conf"));
  }
}
