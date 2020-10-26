package hiveudfs.udtf.zip;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class ZipShortest extends GenericUDTF {
    protected transient ListObjectInspector[] listOIs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 1) {
            throw new UDFArgumentLengthException("The function" + getFuncName() + " accepts at least one arguments");
        }
        listOIs = new ListObjectInspector[argOIs.length];
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < argOIs.length; i++) {
            if (argOIs[i].getCategory() != Category.LIST) {
                throw new UDFArgumentTypeException(i,
                    "Argument " + (i + 1) + " of function " + getFuncName() + " must be an array" +
                        ", but " + argOIs[i].getTypeName() + ".");
            }
            listOIs[i] = (ListObjectInspector) argOIs[i];
            fieldNames.add("col" + (i + 1));
            fieldOIs.add(listOIs[i].getListElementObjectInspector());
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List<?>[] lists = new List<?>[args.length];
        int length = Integer.MAX_VALUE;
        for (int i = 0; i < args.length; i++) {
            List<?> list = listOIs[i].getList(args[i]);
            lists[i] = list;
            length = Math.min(length, list.size());
        }

        for (int i = 0; i < length; i++) {
            Object[] result = new Object[lists.length];
            for (int j = 0; j < lists.length; j++) {
                result[j] = lists[j].get(i);
            }
            forward(result);
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
