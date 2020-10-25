package hiveudfs.udf.arrayutils;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ArrayMedianTest {
    private ArrayMedian udf;
    static final double delta = 0.00010;

    @Before
    public void before() { udf = new ArrayMedian(); }

    @Test
    public void testEvaluateWithArrayOfIntType() throws Exception {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        DoubleObjectInspector resultOI = (DoubleObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1,2,3,4,5)))});
        assertEquals(3.0, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1,2,3,4,5,6)))});
        assertEquals(3.5, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1,3,5,2,4)))});
        assertEquals(3.0, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(6,1,5,2,4,3)))});
        assertEquals(3.5, resultOI.get(result), delta);
    }

    @Test
    public void testEvaluateWithArrayOfDoubleType() throws Exception {
        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
        DoubleObjectInspector resultOI = (DoubleObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1.1,2.2,3.3,4.4,5.5)))});
        assertEquals(3.3, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1.1,2.2,3.3,4.4,5.5,6.6)))});
        assertEquals(3.85, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(1.1,3.3,5.5,2.2,4.4)))});
        assertEquals(3.3, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(6.6,1.1,5.5,2.2,4.4,3.3)))});
        assertEquals(3.85, resultOI.get(result), delta);
    }

    @Test
    public void testEvaluateWithArrayOfDecimalType() throws Exception {
        ObjectInspector decimalOI = PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(decimalOI);
        HiveDecimalObjectInspector resultOI = (HiveDecimalObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(HiveDecimal.create(BigDecimal.valueOf(1.1)), HiveDecimal.create(BigDecimal.valueOf(2.2)), HiveDecimal.create(BigDecimal.valueOf(3.3)), HiveDecimal.create(BigDecimal.valueOf(4.4)), HiveDecimal.create(BigDecimal.valueOf(5.5)))))});
        assertEquals(3.3, resultOI.getPrimitiveJavaObject(result).doubleValue(), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(HiveDecimal.create(BigDecimal.valueOf(1.1)), HiveDecimal.create(BigDecimal.valueOf(2.2)), HiveDecimal.create(BigDecimal.valueOf(3.3)), HiveDecimal.create(BigDecimal.valueOf(4.4)), HiveDecimal.create(BigDecimal.valueOf(5.5)),HiveDecimal.create(BigDecimal.valueOf(6.6)))))});
        assertEquals(3.85, resultOI.getPrimitiveJavaObject(result).doubleValue(), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(HiveDecimal.create(BigDecimal.valueOf(1.1)), HiveDecimal.create(BigDecimal.valueOf(3.3)), HiveDecimal.create(BigDecimal.valueOf(5.5)), HiveDecimal.create(BigDecimal.valueOf(2.2)), HiveDecimal.create(BigDecimal.valueOf(4.4)))))});
        assertEquals(3.3, resultOI.getPrimitiveJavaObject(result).doubleValue(), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList(HiveDecimal.create(BigDecimal.valueOf(6.6)), HiveDecimal.create(BigDecimal.valueOf(1.1)), HiveDecimal.create(BigDecimal.valueOf(5.5)), HiveDecimal.create(BigDecimal.valueOf(2.2)), HiveDecimal.create(BigDecimal.valueOf(4.4)),HiveDecimal.create(BigDecimal.valueOf(3.3)))))});
        assertEquals(3.85, resultOI.getPrimitiveJavaObject(result).doubleValue(), delta);
    }

    @Test
    public void testEvaluateWithArrayOfIntLikeStringType() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        DoubleObjectInspector resultOI = (DoubleObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1","2","3","4","5")))});
        assertEquals(3.0, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1","2","3","4","5","6")))});
        assertEquals(3.5, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1","3","5","2","4")))});
        assertEquals(3.0, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("6","1","5","2","4","3")))});
        assertEquals(3.5, resultOI.get(result), delta);
    }

    @Test
    public void testEvaluateWithArrayOfDoubleLikeStringType() throws Exception {
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        DoubleObjectInspector resultOI = (DoubleObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        Object result;

        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1.1","2.2","3.3","4.4","5.5")))});
        assertEquals(3.3, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1.1","2.2","3.3","4.4","5.5","6.6")))});
        assertEquals(3.85, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("1.1","3.3","5.5","2.2","4.4")))});
        assertEquals(3.3, resultOI.get(result), delta);
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject(new ArrayList<>(Arrays.asList("6.6","1.1","5.5","2.2","4.4","3.3")))});
        assertEquals(3.85, resultOI.get(result), delta);
    }
}
