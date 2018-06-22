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
public class ArrowWriter {
  private int count = 0;
  private VectorSchemaRoot root;
  private List<ArrowFieldWriter> fields;

  public static ArrowWriter create(VectorSchemaRoot root) {
    List<ArrowFieldWriter> fieldWriters = new ArrayList<ArrowFieldWriter>();
    for (FieldVector vec : root.getFieldVectors()) {
      fieldWriters.add(createFieldWriter(vec));
    }
    return new ArrowWriter(root, fieldWriters);
  }

  public void write(Object obj) {
    fields.get(0).write(obj);
    this.count++;
  }

  public void finish() {
    root.setRowCount(this.count);
    for(ArrowFieldWriter field : fields) {
      field.finish();
    }
  }

  public void reset() {
    root.setRowCount(0);
    this.count = 0;
    for(ArrowFieldWriter field : fields) {
      field.reset();
    }
  }

  private static ArrowFieldWriter createFieldWriter(ValueVector vector) {
    Class<?> cls = vector.getClass();
    if(cls.equals(BitVector.class))
      return new BoolWriter(vector);
    else if (cls.equals(TinyIntVector.class))
      return new ByteWriter(vector);
    else if (cls.equals(SmallIntVector.class))
      return new ShortWriter(vector);
    else if (cls.equals(IntVector.class))
      return new IntWriter(vector);
    else if (cls.equals(BigIntVector.class))
      return new LongWriter(vector);
    else if (cls.equals(Float4Vector.class))
      return new FloatWriter(vector);
    else if (cls.equals(Float8Vector.class))
      return new DoubleWriter(vector);
    else if (cls.equals(VarCharVector.class))
      return new StringWriter(vector);
    else if (cls.equals(VarBinaryVector.class))
      return new BinaryWriter(vector);
    else if (cls.equals(DateDayVector.class))
      return new DateWriter(vector);
    else if (cls.equals(TimeStampMicroTZVector.class))
      return new TimestampWriter(vector);
    else if (cls.equals(ListVector.class)) {
      ArrowFieldWriter fieldWriter = createFieldWriter(((ListVector)vector).getDataVector());
      return new ArrayWriter(vector, fieldWriter);
    }
    else
      throw new RuntimeException(String.format("Type '$s' is not supported."));
  }

  private ArrowWriter(VectorSchemaRoot root, List<ArrowFieldWriter> fieldWriters) {
    this.root = root;
    this.fields = fieldWriters;
  }

  private static abstract class ArrowFieldWriter {
    protected ValueVector valueVec;
    protected int count = 0;
    abstract void setNull();
    abstract void setValue(Object obj);

    public void write(Object obj) {
      if(obj == null)
        setNull();
      else
        setValue(obj);
    }
  
    public void finish() {
      this.valueVec.setValueCount(count);
    }
  
    public void reset() {
      if (this.valueVec.getClass().equals(ListVector.class)) {
        ListVector listVec = (ListVector)this.valueVec;
        for (ArrowBuf buffer : listVec.getBuffers(false)) {
          buffer.setZero(0, buffer.capacity());
        }
        listVec.setValueCount(0);
        listVec.setLastSet(0);
      }
      this.count = 0;
    }
  }

  private static class BoolWriter extends ArrowFieldWriter {
    public BoolWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((BitVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((BitVector)this.valueVec).setSafe(this.count, (boolean)obj ? 1 : 0);
    }
  }

  private static class ByteWriter extends ArrowFieldWriter {
    public ByteWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((TinyIntVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((TinyIntVector)this.valueVec).setSafe(this.count, (Byte)obj);
    }
  }

  private static class ShortWriter extends ArrowFieldWriter {
    public ShortWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((SmallIntVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((SmallIntVector)this.valueVec).setSafe(this.count, (Short)obj);
    }
  }

  private static class IntWriter extends ArrowFieldWriter {
    public IntWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((IntVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((IntVector)this.valueVec).setSafe(this.count, (Integer)obj);
    }
  }

  private static class LongWriter extends ArrowFieldWriter {
    public LongWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((BigIntVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((BigIntVector)this.valueVec).setSafe(this.count, (Long)obj);
    }
  }

  private static class FloatWriter extends ArrowFieldWriter {
    public FloatWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((Float4Vector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((Float4Vector)this.valueVec).setSafe(this.count, (Float)obj);
    }
  }

  private static class DoubleWriter extends ArrowFieldWriter {
    public DoubleWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((Float8Vector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((Float8Vector)this.valueVec).setSafe(this.count, (Double)obj);
    }
  }

  private static class StringWriter extends ArrowFieldWriter {
    public StringWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((VarCharVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((VarCharVector)this.valueVec).setSafe(this.count, ((String)obj).getBytes());
    }
  }

  private static class BinaryWriter extends ArrowFieldWriter {
    public BinaryWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((VarBinaryVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((VarBinaryVector)this.valueVec).setSafe(this.count, ((String)obj).getBytes());
    }
  }

  private static class DateWriter extends ArrowFieldWriter {
    public DateWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((DateDayVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((DateDayVector)this.valueVec).setSafe(this.count, (int)obj);
    }
  }

  private static class TimestampWriter extends ArrowFieldWriter {
    public TimestampWriter(ValueVector vector) {
      this.valueVec = vector;
    }

    @Override
    public void setNull() {
      ((TimeStampMicroTZVector)this.valueVec).setNull(this.count);
    }

    @Override
    public void setValue(Object obj) {
      ((TimeStampMicroTZVector)this.valueVec).setSafe(this.count, (Long)obj);
    }
  }

  private static class ArrayWriter extends ArrowFieldWriter {
    private ArrowFieldWriter fieldWriter;

    public ArrayWriter(ValueVector vector, ArrowFieldWriter fieldWriter)
    {
      this.valueVec = vector;
      this.fieldWriter = fieldWriter;
    }

    @Override
    public void setNull(){}

    @Override
    public void setValue(Object obj) {
      ListVector listvec = (ListVector)this.valueVec;
      listvec.startNewValue(count);
      int i = 0;
      for(Object x : (Object[])obj) {
        this.fieldWriter.write(x);
        i++;
      }
      listvec.endValue(count, i);
    }

    @Override 
    public void finish() {
      super.finish();
      this.fieldWriter.finish();
    }
  
    @Override
    public void reset() {
      super.reset();
      this.fieldWriter.reset();
    }
  }
} 