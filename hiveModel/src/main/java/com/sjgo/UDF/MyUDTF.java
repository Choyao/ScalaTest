package com.sjgo.UDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //列名
        List<String> fieldNames = new ArrayList<String>();
        //属性类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 每条数据都调用一次process
     * 处理 "key:value,key:value..."类型的数据
     * 输出 key value两条数据
     * @param objects
     * @throws HiveException
     */
    public void process(Object[] objects) throws HiveException {
        String s = objects.toString();
        String[] split = s.split(",");
        for (int i = 0; i < split.length; i++) {

            String[] keyValue = split[i].split(":");
            //kv格式，两条数据
            forward(keyValue);
        }

    }

    public void close() throws HiveException {

    }
}
