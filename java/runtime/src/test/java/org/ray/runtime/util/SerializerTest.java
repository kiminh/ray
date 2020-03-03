package org.ray.runtime.util;

import org.ray.runtime.serializer.Serializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class SerializerTest {
  @Test
  public void testBasicSerialize() {
    {
      Object[] foo = new Object[]{"hello", (byte) 1, 2.0, (short) 3, 4, 5L, new String[]{"hello", "world"}};
      Object[] bar = Serializer.decode(Serializer.encode(foo), Object[].class);
      Assert.assertEquals(foo[0], bar[0]);
      Assert.assertEquals(foo[1], bar[1]);
      Assert.assertEquals(foo[2], bar[2]);
      Assert.assertEquals(((Number) foo[3]).intValue(), ((Number) bar[3]).intValue());
      Assert.assertEquals(((Number) foo[4]).intValue(), ((Number) bar[4]).intValue());
      Assert.assertEquals(((Number) foo[5]).intValue(), ((Number) bar[5]).intValue());

    }
    {
      Object[][] foo = new Object[][]{{1, 2}, {"3", 4}};
      Assert.expectThrows(RuntimeException.class, () -> {
        Object[][] bar = Serializer.decode(Serializer.encode(foo), Integer[][].class);
      });
      Object[][] bar = Serializer.decode(Serializer.encode(foo), Object[][].class);
      Assert.assertEquals(((Number) foo[0][1]).intValue(), ((Number) bar[0][1]).intValue());
      Assert.assertEquals(foo[1][0], bar[1][0]);
    }
    {
      ArrayList<String> foo = new ArrayList<>();
      foo.add("1");
      foo.add("2");
      ArrayList<String> bar = Serializer.decode(Serializer.encode(foo), String[].class);
      Assert.assertEquals(foo.get(0), bar.get(0));
    }
//    {
//      Map<String, Integer> foo = ImmutableMap.of("1", 1, "2", 2);
//      Map<String, Integer> bar = Serializer.decode(Serializer.encode(foo), foo.getClass());
//    }
  }
}
