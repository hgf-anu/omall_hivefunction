/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: BaseFieldUDF
 * Author: hgf
 * Date: 2019/10/30 0030 下午 20:07
 * Description: UDF
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名修改时间版本号描述
 */
package com.yaxin.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 主要用来解析dwd_base_event_log中的公共字段，事件作为一个JSON对象。
 * 事实上我们可以不适用UDF也能完成需求，但是通过UDF我们可以把错误日志输出到不同文件中，这是系统函数没法做到的。这样可以快速定位错误信息
 */
public class BaseFieldUDF extends UDF{

	public String evaluate( String line,String jsonkeysString ){

		StringBuilder sb = new StringBuilder();

		//1.获取所有key【mid，uv..】
		String[] jsonkeys = jsonkeysString.split(",");

		//2.line 服务器时间|json
		String[] logContents = line.split("\\|");

		//3. 合法性校验
		if( logContents.length != 2 || StringUtils.isBlank(logContents[1]) ){
			return "";
		}

		try{
			//4.对logContens【1】创建json对象
			JSONObject jsonObject = new JSONObject(logContents[1]);

			//5.获取公共字段的json对象,JSON对象里嵌套JSON对象，所以要这么解析
			JSONObject cmJson = jsonObject.getJSONObject("cm");
			for( int i = 0 ; i < jsonkeys.length ; i++ ){
				String jsonkey = jsonkeys[i].trim();
				if( cmJson.has(jsonkey) ){
					sb.append(cmJson.getString(jsonkey)).append("\t");
				}else{
					sb.append("\t");
				}
			}

			//7.拼接事件字段和服务器时间
			sb.append(jsonObject.getString("et")).append("\t");
			sb.append(logContents[0]).append("\t");

		}catch( JSONException e ){
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * 测试UDF功能是否正常
	 */
	public static void main( String[] args ){
		String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
		String x = new BaseFieldUDF().evaluate(line,"mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
		System.out.println(x);
	}

}
