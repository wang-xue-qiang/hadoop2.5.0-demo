package com.zkh.zookeeper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
/**
 * 
 * @Description:测试分布式锁
 * @author wangxueqiang
 * @date 2018年11月6日 下午3:18:23
 *
 */
public class Lock {
	private static final String zkIPs ="bigdata-cdh01.ibeifeng.com:2181,bigdata-cdh02.ibeifeng.com:2181,bigdata-cdh03.ibeifeng.com:2181";
	public static void main(String[] args) throws Exception {
		// 创建zookeeper的客户端
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory
				.newClient(zkIPs, retryPolicy);
		client.start();
		// 创建分布式锁, 锁空间的根节点路径为/curator/lock
		InterProcessMutex mutex = new InterProcessMutex(client, "/curator/lock");
		mutex.acquire();
		// 获得了锁, 进行业务流程
		System.out.println("Enter mutex");
		// 完成业务流程, 释放锁
		mutex.release();
		// 关闭客户端
		client.close();
	}
}
