package com.zkh.zookeeper;

import org.apache.zookeeper.AsyncCallback.StringCallback;
/**
 * 
 * @Description:异步创建节点回调函数
 * @author wangxueqiang
 * @date 2018年9月10日 上午11:11:48
 *
 */
public class CreateCallBack  implements StringCallback{
	public void processResult(int rc, String path, Object ctx, String name) {
		 System.out.println("创建节点：" + path);
	     System.out.println((String) ctx);	
	}
}
