package hiveudfs.udf.date;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class StrftimeTest {
    private Strftime udf;
    private Calendar calendar = Calendar.getInstance();

    @Before
    public void before() {
        udf = new Strftime();
    }

    @Test
    public void testEvaluateWithStringTypeAsDateNoDash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("20180102", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018/01/02", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringTypeAsDateWithHyphen() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("20180102", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018/01/02", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringTypeAsDateWithSlash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("20180102", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018/01/02", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringTypeAsTimestampNoDash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102 030405"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("20180102 030405", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102 030405"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102 030405"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018/01/02 03:04:05", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringTypeAsTimestampWithHyphen() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("20180102 000000", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018/01/02 00:00:00", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringTypeAsTimestampWithSlash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02 03:04:05"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("20180102 030405", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02 03:04:05"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02 03:04:05"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018/01/02 03:04:05", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithDateType() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("20180102", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018/01/02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("20180102 000000", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new DateWritable(Date.valueOf("2018-01-02"))), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018/01/02 00:00:00", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithTimestampType() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        calendar.set(2018, Calendar.JANUARY, 2, 3, 4, 5);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("20180102", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018/01/02", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyyMMdd'T'HHmmss")});
        assertEquals("20180102T030405", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy-MM-dd'T'HH:mm:ss")});
        assertEquals("2018-01-02T03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy/MM/dd'T'HH:mm:ss")});
        assertEquals("2018/01/02T03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("20180102 030405", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new TimestampWritable(new Timestamp(calendar.getTimeInMillis()))), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018/01/02 03:04:05", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateAsOtherFormat() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("yyyy.MM.dd G 'at' HH:mm:ss z")});
        assertEquals("2018.01.02 AD at 00:00:00 GMT", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("EEE, MMM d, ''yy")});
        assertEquals("Tue, Jan 2, '18", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("h:mm a")});
        assertEquals("12:00 AM", resultOI.getPrimitiveJavaObject(result));
    }
}
