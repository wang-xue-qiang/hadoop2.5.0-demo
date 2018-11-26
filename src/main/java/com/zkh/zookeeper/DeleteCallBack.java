package com.zkh.zookeeper;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
/**
 * 
 * @Description：异步节点删除
 * @author wangxueqiang
 * @date 2018年9月10日 下午1:09:19
 *
 */
public class DeleteCallBack  implements VoidCallback{
	public void processResult(int rc, String path, Object ctx) {
		 System.out.println("删除节点：" + path + " 成功...");
	     System.out.println((String) ctx);
	}

}
