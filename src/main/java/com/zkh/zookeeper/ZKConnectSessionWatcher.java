package com.zkh.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @description: zookeeper 恢复之前的会话连接demo演示
 **/
public class ZKConnectSessionWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZKConnectSessionWatcher.class);
    private static final String zkServerIp = "bigdata-cdh01.ibeifeng.com:2181,bigdata-cdh02.ibeifeng.com:2181,bigdata-cdh03.ibeifeng.com:2181";// 集群模式则是多个ip
    private static final Integer timeout = 5000; // 超时时间
    public void process(WatchedEvent watchedEvent) {// Watch事件通知
        logger.warn("接收到watch通知：{}", watchedEvent);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // 实例化zookeeper客户端
        ZooKeeper zooKeeper = new ZooKeeper(zkServerIp, timeout, new ZKConnectSessionWatcher());
        logger.warn("客户端开始连接zookeeper服务器...");
        logger.warn("连接状态：{}", zooKeeper.getState());
        Thread.sleep(2000);
        logger.warn("连接状态：{}", zooKeeper.getState());
        // 记录本次会话的sessionId
        long sessionId = zooKeeper.getSessionId();
        // 转换成16进制进行打印
        logger.warn("sid：{}", "0x" + Long.toHexString(sessionId));
        // 记录本次会话的session密码
        byte[] sessionPassword = zooKeeper.getSessionPasswd();
        Thread.sleep(200);
        // 开始会话重连
        logger.warn("开始会话重连...");
        // 加上sessionId和password参数去实例化zookeeper客户端
        ZooKeeper zkSession = new ZooKeeper(zkServerIp, timeout, new ZKConnectSessionWatcher(), sessionId, sessionPassword);
        logger.warn("重新连接状态zkSession：{}", zkSession.getState());
        Thread.sleep(2000);
        logger.warn("重新连接状态zkSession：{}", zkSession.getState());
    }
}