package org.ray.streaming.runtime.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.ray.api.Ray;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;
import org.slf4j.Logger;

public class RayUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RayUtils.class);

  public static List<NodeInfo> getNodeInfoList() {
    List<NodeInfo> nodeInfoList;
    if (TestHelper.isUTPattern()) {
      // just for UT
      nodeInfoList = getAllNodeInfoMock();
    } else {
      nodeInfoList = Ray.getRuntimeContext().getAllNodeInfo();
    }
    return nodeInfoList;
  }

  public static Map<UniqueId, NodeInfo> getNodeInfoMap() {
    return getNodeInfoList().stream().filter(nodeInfo -> nodeInfo.isAlive).collect(
        Collectors.toMap(nodeInfo -> nodeInfo.nodeId, nodeInfo -> nodeInfo));
  }

  private static List<NodeInfo> getAllNodeInfoMock() {
    return TestHelper.mockContainerResources();
  }

  private static Map<String, UniqueId> getNodeName2IdsMock() {
    return TestHelper.mockNodeName2Ids(getAllNodeInfoMock());
  }

  public static Map<String, UniqueId> getNodeName2IdsMap() {
    LOG.info("Get nodeName2IdsMap.");
    if (TestHelper.isUTPattern()) {
      return getNodeName2IdsMock();
    } else {
      //TODO: @qw implements this method
      //return Ray.getRuntimeContext().getNodeName2Ids();
      return Maps.newHashMap();
    }
  }
}
