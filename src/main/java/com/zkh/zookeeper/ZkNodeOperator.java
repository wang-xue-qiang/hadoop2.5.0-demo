package com.zkh.zookeeper;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
public class ZkNodeOperator implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ZkNodeOperator.class);
    private static final String zkServerIp = "bigdata-cdh01.ibeifeng.com:2181,bigdata-cdh02.ibeifeng.com:2181,bigdata-cdh03.ibeifeng.com:2181";
    private static final Integer timeout = 5000;// 超时时间
    private static final ZkNodeOperator zkServer = new ZkNodeOperator(zkServerIp);
    private ZooKeeper zooKeeper;
    public  ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
    public ZkNodeOperator() {}
    public ZkNodeOperator(String connectStr) {
        try {
            // 在使用该构造器的时候，实例化zk客户端对象
            zooKeeper = new ZooKeeper(connectStr, timeout, new ZkNodeOperator());
        } catch (IOException e) {
            e.printStackTrace();
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }
    /**
     * 
     * @Description:Watch事件通知方法 
     * @param watchedEvent
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     * @author wangxueqiang
     * @date 2018年9月10日 上午11:21:00
     *
     */
    public void process(WatchedEvent watchedEvent) {
        logger.warn("接收到watch通知：{}", watchedEvent);
    }
    /**
     * 
     * @Description: 创建同步节点
     * @param path
     * @param data
     * @param acls
     * @author wangxueqiang
     * @date 2018年9月10日 上午11:15:12
     *
     */
    public void createSynZKNode(String path, byte[] data, List<ACL> acls) {
        String result = "";
        try {
            /**
             * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
             * 参数：
             * path：节点创建的路径
             * data：节点所存储的数据的byte[]
             * acl：控制权限策略
             *          Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             *          CREATOR_ALL_ACL --> auth:user:password:cdrwa
             * createMode：节点类型, 是一个枚举
             *          PERSISTENT：持久节点
             *          PERSISTENT_SEQUENTIAL：持久顺序节点
             *          EPHEMERAL：临时节点
             *          EPHEMERAL_SEQUENTIAL：临时顺序节点
             */
            // 同步创建zk节点，节点类型为临时节点
            result = zooKeeper.create(path, data, acls, CreateMode.EPHEMERAL);
            System.out.println("创建节点：\t" + result + "\t成功...");
            Thread.sleep(2000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * 
     * @Description: 创建异步节点
     * @param path
     * @param data
     * @param acls
     * @author wangxueqiang
     * @date 2018年9月10日 上午11:07:27
     *
     */
    public void createAsynZKNode(String path, byte[] data, List<ACL> acls) {
        try {
            // 异步步创建zk节点，节点类型为持久节点
            String ctx = "{'create':'success'}";
            zooKeeper.create(path, data, acls, CreateMode.PERSISTENT, new CreateCallBack(), ctx);
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * 
     * @Description: 修改同步节点
     * @param data
     * @throws Exception
     * @author wangxueqiang
     * @date 2018年9月10日 上午11:27:54
     *
     */
    public void alertSynZKNode(ZkNodeOperator zkServer,String nodePath,byte[] data) throws Exception{
    	Stat status = zkServer.getZooKeeper().setData(nodePath, data, 0);
        // 通过Stat对象可以获取znode所有的状态属性，这里以version为例
        System.out.println("修改成功，当前数据版本为：" + status.getVersion());
    } 
    /**
     * 
     * @Description: 修改异步节点
     * @param zkServer
     * @param data
     * @throws Exception
     * @author wangxueqiang
     * @date 2018年9月10日 上午11:32:50
     *
     */
    public void alertAsynZKNode(ZkNodeOperator zkServer,String nodePath,byte[] data) throws Exception{
    	String ctx = "{'alter':'success'}";
    	zkServer.getZooKeeper().setData(nodePath, data, 0,new AlterCallBack(), ctx);
    	Thread.sleep(2000);
    }
    /**
     * 
     * @Description: 同步节点删除
     * @param nodePath
     * @param version
     * @author wangxueqiang
     * @throws IOException 
     * @date 2018年9月10日 上午11:56:23
     *
     */
    public void deleteSynZKNode(String nodePath,int version) throws Exception{
    	ZooKeeper zooKeeper = new ZooKeeper(zkServerIp, timeout, new ZkNodeOperator());
    	zooKeeper.delete(nodePath, version);
    }
    /**
     * 
     * @Description: 异步节点删除
     * @param nodePath
     * @param version
     * @throws IOException
     * @author wangxueqiang
     * @throws Exception 
     * @date 2018年9月10日 下午1:12:09
     *
     */
    public void deleteAsynZKNode(String nodePath,int version) throws Exception{
    	String ctx = "{'delete':'success'}";
    	ZooKeeper zooKeeper = new ZooKeeper(zkServerIp, timeout, new ZkNodeOperator());
    	zooKeeper.delete(nodePath, version,new DeleteCallBack(),ctx);
    	Thread.sleep(2000);
        zooKeeper.close();
    }
    /**
     * 
     * @Description: 同步获取节点数据
     * @param nodePath
     * @throws Exception
     * @author wangxueqiang
     * @date 2018年9月10日 下午1:23:21
     *
     */
    public void getSynData(String nodePath) throws Exception{
    	ZooKeeper zooKeeper = new ZooKeeper(zkServerIp, timeout, new ZkNodeOperator());
        /**
         * 参数：
         * path：节点路径
         * watch：true或者false，注册一个watch事件
         * stat：状态，我们可以通过这个对象获取节点的状态信息
         */
        byte[] resByte = zooKeeper.getData(nodePath, true, new Stat());
        String result = new String(resByte);
        System.out.println(nodePath+"节点的数据: " + result);
        zooKeeper.close();
    }
    
    public static void main(String[] args) throws Exception {
        /*同步创建修改节点*/
        //zkServer.createSynZKNode("/testSynNode", "testSynNode-data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        //zkServer.alertSynZKNode(zkServer,"/testSynNode", "this is new data".getBytes());
        /*同步创建修改节点*/
        
        /*异步创建修改节点*/
        zkServer.createAsynZKNode("/testAsynNode", "testAsynNode-data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);  
        //zkServer.alertAsynZKNode(zkServer,"/testAsynNode", "asynchronous-data".getBytes());
        /*异步创建修改节点*/
        
        /*删除同步节点*/
        //new ZkNodeOperator().deleteSynZKNode("/testSynNode", 0);
        /*删除同步节点*/
        
        /*异步节点删除*/
        //new ZkNodeOperator().deleteAsynZKNode("/testAsynNode", 1);
        /*异步节点删除*/
        
        /*同步获取节点数*/
        new ZkNodeOperator().getSynData("/testAsynNode");
        /*同步获取节点数*/
        
    }
}