package com.zkh.zookeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;
/**
 * 
 * @Description:异步修改节点回调函数
 * @author wangxueqiang
 * @date 2018年9月10日 上午11:34:26
 *
 */
public class AlterCallBack  implements StatCallback{
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		 System.out.println("修改节点：" + path + "成功...");
	        // 通过Stat对象可以获取znode所有的状态属性，这里以version为例
	        System.out.println("当前数据版本为：" + stat.getVersion());
	        System.out.println((String) ctx);	
	}
}
