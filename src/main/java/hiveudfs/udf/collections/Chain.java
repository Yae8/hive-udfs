package hiveudfs.udf.collections;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

import java.util.List;
import java.util.Map;

public class Chain extends GenericUDF {
    protected transient ListObjectInspector[] listOIs;
    protected transient MapObjectInspector[] mapOIs;
    protected transient StandardListObjectInspector stdListOI;
    protected transient StandardMapObjectInspector stdMapOI;
    protected transient Category category;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 1) {
            throw new UDFArgumentException("The function " + getUdfName() + " takes arguments at least one.");
        }

        switch (arguments[0].getCategory()) {
            case LIST:
                category = Category.LIST;
                listOIs = new ListObjectInspector[arguments.length];
                break;
            case MAP:
                category = Category.MAP;
                mapOIs = new MapObjectInspector[arguments.length];
                break;
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " only takes maps or lists.");
        }

        ObjectInspector first = arguments[0];
        for (int i = 0; i < arguments.length; i++) {
            ObjectInspector inputOI = arguments[i];
            if (!ObjectInspectorUtils.compareTypes(inputOI, first)) {
                throw new UDFArgumentException("The function " + getUdfName() + " must be all maps of the same type");
            }
            switch (category) {
                case LIST:
                    listOIs[i] = (ListObjectInspector) inputOI;
                    break;
                case MAP:
                    mapOIs[i] = (MapObjectInspector) inputOI;
                    break;
                default:
                    throw new UDFArgumentException("The function " + getUdfName() + " only takes maps or lists.");
            }
        }

        switch (category) {
            case LIST:
                stdListOI = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
                    listOIs[0], ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                return stdListOI;
            case MAP:
                stdMapOI = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
                    mapOIs[0], ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                return stdMapOI;
            default:
                throw new UDFArgumentException("The function " + getUdfName() + " only takes maps or lists.");
        }
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

    public Object evaluateList(DeferredObject[] arguments) throws HiveException {
        Object result = stdListOI.create(0);
        int index = 0;
        int currentSize = 0;
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].get() == null) {
                continue;
            }
            List<?> list = listOIs[i].getList(arguments[i].get());
            currentSize += list.size();
            stdListOI.resize(result, currentSize);
            for (Object object : list) {
                Object element = ObjectInspectorUtils.copyToStandardObject(
                    object,
                    listOIs[i].getListElementObjectInspector(),
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                stdListOI.set(result, index,  element);
                index++;
            }
        }
        return result;
    }

    public Object evaluateMap(DeferredObject[] arguments) throws HiveException {
        Object result = stdMapOI.create();
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].get() == null) {
                continue;
            }
            Map<?, ?> map = mapOIs[i].getMap(arguments[i].get());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = ObjectInspectorUtils.copyToStandardObject(
                    entry.getKey(),
                    mapOIs[i].getMapKeyObjectInspector(),
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                Object value = ObjectInspectorUtils.copyToStandardObject(
                    entry.getValue(),
                    mapOIs[i].getMapValueObjectInspector(),
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                stdMapOI.put(result, key, value);
            }
        }
        return result;
    }

    @Override
    protected String getFuncName() {
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return getStandardDisplayString(getFuncName(), children);
    }
}
