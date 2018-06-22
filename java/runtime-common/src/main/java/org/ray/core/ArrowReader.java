package org.ray.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.*;
import io.netty.buffer.ArrowBuf;

/**
 * class used to write data into Arrow structure
 */
public class ArrowReader {
  private int count = 0;
  private VectorSchemaRoot root;
  private List<ArrowFieldReader> fields;

  public static ArrowReader create(VectorSchemaRoot root) {
    List<ArrowFieldReader> fieldReader = new ArrayList<ArrowFieldReader>();
    for (FieldVector vec : root.getFieldVectors()) {
      fieldReader.add(createFieldReader(vec));
    }
    return new ArrowReader(root, fieldReader);
  }

  public Object read() {
    // todo
    return fields.get(0).getValue(0);
  }

  private static ArrowFieldReader createFieldReader(ValueVector vector) {
    Class<?> cls = vector.getClass();
    if(cls.equals(BitVector.class))
      return new BoolReader(vector);
    else if (cls.equals(TinyIntVector.class))
      return new ByteReader(vector);
    else if (cls.equals(SmallIntVector.class))
      return new ShortReader(vector);
    else if (cls.equals(IntVector.class))
      return new IntReader(vector);
    else if (cls.equals(BigIntVector.class))
      return new LongReader(vector);
    else if (cls.equals(Float4Vector.class))
      return new FloatReader(vector);
    else if (cls.equals(Float8Vector.class))
      return new DoubleReader(vector);
    else if (cls.equals(VarCharVector.class))
      return new StringReader(vector);
    else if (cls.equals(VarBinaryVector.class))
      return new BinaryReader(vector);
    else if (cls.equals(DateDayVector.class))
      return new DateReader(vector);
    else if (cls.equals(TimeStampMicroTZVector.class))
      return new TimestampReader(vector);
    else if (cls.equals(ListVector.class)) {
      ArrowFieldReader fieldWriter = createFieldReader(((ListVector)vector).getDataVector());
      return new ArrayReader(vector, fieldWriter);
    }
    else
      throw new RuntimeException(String.format("Type '$s' is not supported."));
  }

  private ArrowReader(VectorSchemaRoot root, List<ArrowFieldReader> fieldWriters) {
    this.root = root;
    this.fields = fieldWriters;
  }

  private static abstract class ArrowFieldReader {
    protected ValueVector valueVec;
    abstract Object getValue(int index);
  }

  private static class BoolReader extends ArrowFieldReader {
    public BoolReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((BitVector)this.valueVec).get(index) == 1 ? true : false;
    }
  }

  private static class ByteReader extends ArrowFieldReader {
    public ByteReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((TinyIntVector)this.valueVec).get(index);
    }
  }

  private static class ShortReader extends ArrowFieldReader {
    public ShortReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((SmallIntVector)this.valueVec).get(index);
    }
  }

  private static class IntReader extends ArrowFieldReader {
    public IntReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((IntVector)this.valueVec).get(index);
    }
  }

  private static class LongReader extends ArrowFieldReader {
    public LongReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((BigIntVector)this.valueVec).get(index);
    }
  }

  private static class FloatReader extends ArrowFieldReader {
    public FloatReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((Float4Vector)this.valueVec).get(index);
    }
  }

  private static class DoubleReader extends ArrowFieldReader {
    public DoubleReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((Float8Vector)this.valueVec).get(index);
    }
  }

  private static class StringReader extends ArrowFieldReader {
    public StringReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((VarCharVector)this.valueVec).get(index);
    }
  }

  private static class BinaryReader extends ArrowFieldReader {
    public BinaryReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((VarBinaryVector)this.valueVec).get(index);
    }
  }

  private static class DateReader extends ArrowFieldReader {
    public DateReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((DateDayVector)this.valueVec).get(index);
    }
  }

  private static class TimestampReader extends ArrowFieldReader {
    public TimestampReader(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public Object getValue(int index) {
      return ((TimeStampMicroTZVector)this.valueVec).get(index);
    }
  }

  private static class ArrayReader extends ArrowFieldReader {
    private ArrowFieldReader fieldReader;

    public ArrayReader(ValueVector vector, ArrowFieldReader fieldReader)
    {
      this.valueVec = vector;
      this.fieldReader = fieldReader;
    }

    @Override
    public Object getValue(int index) {
      ListVector listvec = (ListVector)this.valueVec;
      List<Object> res = new ArrayList<>();
      for(int i = 0; i < listvec.getValueCount(); i++) {
        res.add(this.fieldReader.getValue(i));
      }
      return res;
    }
  }
}