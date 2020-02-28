package org.ray.api.test;

import org.nustaq.serialization.*;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.runtime.actor.NativeRayActorSerializer;
import org.ray.runtime.util.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.management.relation.RoleUnresolved;
import java.io.*;
import java.util.Map;

public class TestFst {

  static FSTConfiguration conf;

  public static class A implements Externalizable {
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(x.getBytes());
    }
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      x = (String) in.readObject();
      throw new RuntimeException();
    }

    public String x = "hello";
  }

  public static class B implements Serializable {
    A a = new A();
    A xx = new A();
  }


  public static class ASer extends FSTBasicObjectSerializer {
    @Override
    public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo,
                            FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
      out.writeObject(toWrite);
    }

    @Override
    public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo,
                           FSTClazzInfo.FSTFieldInfo referencedBy) throws Exception {
      throw new RuntimeException("read exception");
    }

  }

  static {
    conf = FSTConfiguration.createDefaultConfiguration();
    conf.registerSerializer(A.class, new ASer(), false);
  }

  @Test
  public void testA() {
    B b = new B();
    byte[] bytes = conf.asByteArray(b);
    B result = (B)conf.asObject(bytes);
    Assert.assertEquals(b, result);
  }

}
