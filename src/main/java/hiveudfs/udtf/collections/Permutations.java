package hiveudfs.udtf.collections;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;


public class Permutations extends GenericUDTF {
    private transient ObjectInspector[] inputOIs;
    protected transient ObjectInspectorConverters.Converter converter;
    protected transient Object[] forwardObjects = new Object[1];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 1 || argOIs.length > 2) {
            throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to two arguments");
        }

        inputOIs = new ObjectInspector[argOIs.length];
        if (argOIs[0].getCategory() != Category.LIST) {
            throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function " + getFuncName() + " must be list.");
        }
        ListObjectInspector listOI = (ListObjectInspector) argOIs[0];
        inputOIs[0] = argOIs[0];

        if (argOIs.length == 2) {
            if (argOIs[1].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function " + getFuncName() + " must be primitive.");
            }
            PrimitiveObjectInspector primOI = (PrimitiveObjectInspector) argOIs[1];
            inputOIs[1] = argOIs[1];
            switch (primOI.getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                    break;
                default:
                    throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function " + getFuncName() + " must be " +
                            serdeConstants.INT_TYPE_NAME +
                            ", but " + argOIs[1].getTypeName() + ".");
            }
            converter = ObjectInspectorConverters.getConverter(primOI, PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("array");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(listOI.getListElementObjectInspector()));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        ListObjectInspector listOI;
        List<Object> list;
        int r;
        switch (args.length) {
            case 1:
                listOI = (ListObjectInspector) inputOIs[0];
                list = (List<Object>) listOI.getList(args[0]);
                r = list.size();
                break;
            case 2:
                listOI = (ListObjectInspector) inputOIs[0];
                list = (List<Object>) listOI.getList(args[0]);
                r = ((IntWritable) converter.convert(args[1])).get();
                break;
            default:
                throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts one to two arguments");
        }
        if (list == null) {
            return;
        }
        if (r == 0) {
            return;
        }
        permutations(list, r, new ArrayList<>());
    }

    private void permutations(List<Object> candidates, int r, List<Object> selected) throws HiveException {
        if (candidates.size() < r) {
            return;
        }
        for (int i = 0; i < candidates.size(); i++) {
            ArrayList<Object> newSelected = new ArrayList<>(selected);
            newSelected.add(candidates.get(i));
            if (r > 1) {
                ArrayList<Object> newCandidates = new ArrayList<>();
                newCandidates.addAll(candidates.subList(0, i));
                newCandidates.addAll(candidates.subList(i + 1, candidates.size()));
                permutations(
                    newCandidates,
                    r - 1,
                    newSelected);
            } else {
                forwardObjects[0] = newSelected;
                forward(forwardObjects);
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }

    @Override
    public String toString() {
        return this.getFuncName();
    }

    protected String getFuncName() {
        return this.getClass().getSimpleName().toLowerCase();
    }
}
