package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class WritableTimestampObjectInspector  extends AbstractPrimitiveWritableObjectInspector implements SettableTimestampObjectInspector {
    public WritableTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
        return o == null ? null : (TimestampWritableV2)o;
    }

    public Timestamp getPrimitiveJavaObject(Object o) {
        // https://issues.apache.org/jira/browse/HIVE-22224
        if (o instanceof LongWritable) {
            return Timestamp.ofEpochMilli(((LongWritable)o).get());
        }
        return o == null ? null : ((TimestampWritableV2)o).getTimestamp();
    }

    public Object copyObject(Object o) {
        return o == null ? null : new TimestampWritableV2((TimestampWritableV2)o);
    }

    public Object set(Object o, byte[] bytes, int offset) {
        ((TimestampWritableV2)o).set(bytes, offset);
        return o;
    }

    /** @deprecated */
    @Deprecated
    public Object set(Object o, java.sql.Timestamp t) {
        if (t == null) {
            return null;
        } else {
            ((TimestampWritableV2)o).set(Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
            return o;
        }
    }

    public Object set(Object o, Timestamp t) {
        if (t == null) {
            return null;
        } else {
            ((TimestampWritableV2)o).set(t);
            return o;
        }
    }

    public Object set(Object o, TimestampWritableV2 t) {
        if (t == null) {
            return null;
        } else {
            ((TimestampWritableV2)o).set(t);
            return o;
        }
    }

    public Object create(byte[] bytes, int offset) {
        return new TimestampWritableV2(bytes, offset);
    }

    public Object create(java.sql.Timestamp t) {
        return new TimestampWritableV2(Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    }

    public Object create(Timestamp t) {
        return new TimestampWritableV2(t);
    }
}