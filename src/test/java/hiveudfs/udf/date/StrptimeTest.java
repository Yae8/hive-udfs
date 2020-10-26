package hiveudfs.udf.date;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StrptimeTest {
    private Strptime udf;

    @Before
    public void before() { udf = new Strptime(); }

    @Test
    public void testEvaluateWithStringAsDateNoDash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("2018-01-02 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20190305"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("2019-03-05 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20201031"), new DeferredJavaObject("yyyyMMdd")});
        assertEquals("2020-10-31 00:00:00", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringAsDateWithHyphen() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2018-01-02 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2019-03-05"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2019-03-05 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2020-10-31"), new DeferredJavaObject("yyyy-MM-dd")});
        assertEquals("2020-10-31 00:00:00", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringAsDateWithSlash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2018-01-02 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2019/03/05"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2019-03-05 00:00:00", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2020/10/31"), new DeferredJavaObject("yyyy/MM/dd")});
        assertEquals("2020-10-31 00:00:00", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringAsTimestampNoDash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20180102 030405"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20190305 070911"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("2019-03-05 07:09:11", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("20201031 235959"), new DeferredJavaObject("yyyyMMdd HHmmss")});
        assertEquals("2020-10-31 23:59:59", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringAsTimestampWithHyphen() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018-01-02 03:04:05"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2019-03-05 07:09:11"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2019-03-05 07:09:11", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2020-10-31 23:59:59"), new DeferredJavaObject("yyyy-MM-dd HH:mm:ss")});
        assertEquals("2020-10-31 23:59:59", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithStringAsTimestampWithSlash() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2018/01/02 03:04:05"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2018-01-02 03:04:05", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2019/03/05 07:09:11"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2019-03-05 07:09:11", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("2020/10/31 23:59:59"), new DeferredJavaObject("yyyy/MM/dd HH:mm:ss")});
        assertEquals("2020-10-31 23:59:59", resultOI.getPrimitiveJavaObject(result));
    }
}
