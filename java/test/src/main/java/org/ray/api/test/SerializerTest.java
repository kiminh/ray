package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.core.Serializer;


@RunWith(MyRunner.class)
public class SerializerTest {

  public static void main(String[] args) {
    Boolean a = true;
    Serializer.serialize(a);
  }

}
