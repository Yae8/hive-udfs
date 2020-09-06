package hiveudfs.udf.setmetric;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

abstract class SetMetricBase extends GenericUDF {

    protected Category category;
    protected ListObjectInspector list1OI;
    protected ListObjectInspector list2OI;
    protected MapObjectInspector map1OI;
    protected MapObjectInspector map2OI;
    protected PrimitiveObjectInspector primitive1OI;
    protected PrimitiveObjectInspector primitive2OI;
    protected BooleanWritable result;

    abstract Object evaluateList(DeferredObject[] arguments) throws HiveException;
    abstract Object evaluateMap(DeferredObject[] arguments) throws HiveException;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes 2 maps or lists.");
        }

        ObjectInspector firstOI = arguments[0];
        ObjectInspector secondOI = arguments[0];
        category = firstOI.getCategory();

        if (secondOI.getCategory() != category) {
            throw new UDFArgumentException("The function " + getUdfName() + " must either be all maps or all lists of the same type.");
        }

        switch (category) {
            case LIST:
                list1OI = (ListObjectInspector) firstOI;
                list2OI = (ListObjectInspector) secondOI;
                if (list1OI.getListElementObjectInspector().getCategory() != Category.PRIMITIVE || list2OI.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
                    throw new UDFArgumentException("The function " + getUdfName() + " takes only maps or lists of primitives.");
                }
                primitive1OI = (PrimitiveObjectInspector) list1OI.getListElementObjectInspector();
                primitive2OI = (PrimitiveObjectInspector) list2OI.getListElementObjectInspector();
                if (primitive1OI.getPrimitiveCategory().compareTo(primitive2OI.getPrimitiveCategory()) != 0) {
                    throw new UDFArgumentException("The function " + getUdfName() + " must either be all maps or all lists of the same type");
                }
                break;
            case MAP:
                map1OI = (MapObjectInspector) firstOI;
                map2OI = (MapObjectInspector) secondOI;
                if (map1OI.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE || map2OI.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE) {
                    throw new UDFArgumentException("The function " + getUdfName() + " takes only maps or lists of primitives.");
                }
                primitive1OI = (PrimitiveObjectInspector) map1OI.getMapKeyObjectInspector();
                primitive2OI = (PrimitiveObjectInspector) map2OI.getMapKeyObjectInspector();
                if (primitive1OI.getPrimitiveCategory().compareTo(primitive2OI.getPrimitiveCategory()) != 0) {
                    throw new UDFArgumentException("The function " + getUdfName() + " must either be all maps or all lists of the same type");
                }
                break;
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " takes only maps or lists.");
        }

        result = new BooleanWritable(false);
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        switch (category) {
            case LIST:
                return evaluateList(arguments);
            case MAP:
                return evaluateMap(arguments);
            default:
                throw new HiveException("Only maps or lists are supported.");
        }
    }
}
