/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: EventJsonUDTF
 * Author: hgf
 * Date: 2019/10/30 0030 下午 20:51
 * Description: UDTF
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名修改时间版本号描述
 */
package com.yaxin.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br>
 * 〈UDTF〉
 *
 * @author hgf
 * @create 2019/10/30 0030
 * @since 1.0.0
 */
public class EventJsonUDTF extends GenericUDTF{
	@Override
	public StructObjectInspector initialize( ObjectInspector[] argOIs ) throws UDFArgumentException{
		List< String > fieldNames = new ArrayList<>();
		List< ObjectInspector > fieldType = new ArrayList<>();

		fieldNames.add("event_name");
		fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		fieldNames.add("event_json");
		fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldType);
	}

	@Override
	public void process( Object[] objects ) throws HiveException{

		//获取输入数据，一进一出（也可以多进多出）
		String input = objects[0].toString();

		if( StringUtils.isBlank(input) ){
			return;
		}else{
			try{
				JSONArray ja = new JSONArray(input);
				if( ja == null ){
					return;
				}

				for( int i = 0 ; i < ja.length() ; i++ ){
					String[] results = new String[2];
					//这里做一个try+catch，增加操作的容错性，不会产生错误的时候马上崩溃
					try{
						//设计到往里面一层走的用getJSONObject()/Array()，取具体值的用getString("字段名")
						results[0] = ja.getJSONObject(i).getString("en");
						//这里保存的格式是事件名+其他信息
						results[1] = ja.getString(i);
					}catch( JSONException e ){
						e.printStackTrace();
						continue;
					}
					//交给框架
					forward(results);
				}
			}catch( JSONException e ){
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() throws HiveException{

	}
}
