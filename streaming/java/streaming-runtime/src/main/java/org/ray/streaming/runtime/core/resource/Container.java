package org.ray.streaming.runtime.core.resource;

import java.io.Serializable;

import com.google.common.base.MoreObjects;
import org.ray.api.id.UniqueId;

public class Container implements Serializable {

  private ContainerID id;

  private String address;

  private String hostname;

  /**
   * One resource can owner some containers, one container only belong to one resource.
   */
  private Resource resource;

  /**
   * one raylet has one unique node id
   */
  private UniqueId nodeId;

  public Container() {
  }

  public Container(String address, UniqueId nodeId, String hostname) {
    this.id = new ContainerID();
    this.address = address;
    this.hostname = hostname;
    this.nodeId = nodeId;
  }

  public void setId(ContainerID id) {
    this.id = id;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public ContainerID getId() {
    return id;
  }

  public String getName() {
    return id.toString();
  }

  public String getAddress() {
    return address;
  }

  public Resource getResource() {
    return resource;
  }

  public UniqueId getNodeId() {
    return nodeId;
  }

  public void setNodeId(UniqueId nodeId) {
    this.nodeId = nodeId;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("address", address)
            .add("hostname", hostname)
            .add("resource", resource)
            .add("nodeId", nodeId)
            .toString();

  }
}