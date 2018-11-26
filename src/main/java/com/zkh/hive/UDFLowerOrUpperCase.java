package com.zkh.hive;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 
 * create function lowerorupper as 'com.zkh.hive.UDFLowerOrUpperCase' using jar 'hdfs://XX:8020/hive/transformer/lowerorupper-0.0.1.jar';  
 * @Description:自定义UDF，要求继承udf，并重载实现evaluate方法 一个输入对应一个输出
 * @author wangxueqiang
 * @date 2018年10月31日 上午2:33:17
 *
 */
public class UDFLowerOrUpperCase extends UDF {
	/**
	 * 转换小写
	 * 
	 * @param t
	 * @return
	 */
	public Text evaluate(Text t) {
		// 默认进行小写转换
		return this.evaluate(t, "lower");
	}

	/**
	 * 对参数t进行大小写转换
	 * 
	 * @param t
	 * @param lowerOrUpper
	 *            如果该值为lower，则进行小写转换，如果该值为upper则进行大写转换，其他情况不进行转换。
	 * @return
	 */
	public Text evaluate(Text t, String lowerOrUpper) {
		if (t == null) {
			return t;
		}
		if ("lower".equals(lowerOrUpper)) {
			return new Text(t.toString().toLowerCase());
		} else if ("upper".equals(lowerOrUpper)) {
			return new Text(t.toString().toUpperCase());
		}
		// 转换参数错误的情况下，直接返回原本的值
		return t;
	}
}
