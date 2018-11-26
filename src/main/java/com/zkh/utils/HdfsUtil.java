package com.zkh.utils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 
 * @Description: hdfs 操作工具类
 * @author wangxueqiang
 * @date 2018年10月31日 上午2:05:16
 *
 */
public class HdfsUtil {

	private static final String HDFSURI = "hdfs://ns1";
	private static final String HDFSUSER = "root";

	/**
	 * 
	 * @Description: 获取文件系统
	 * @return
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午9:48:36
	 *
	 */
	public static FileSystem getFileSystem() {
		Configuration conf = new Configuration();
		conf.setBoolean("dfs.permissions", false);
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(new URI(HDFSURI), conf, HDFSUSER);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return hdfs;
	}

	/**
	 * 
	 * @Description: 创建文件系统
	 * @param path
	 * @author wangxueqiang
	 * @throws Exception
	 * @date 2018年9月19日 上午9:52:43
	 *
	 */
	public static void mkdirsFiles(String path) throws Exception {
		FileSystem hdfs = getFileSystem();
		boolean flag = hdfs.mkdirs(new Path(path));
		System.out.println("是否创建成功：" + flag);
		hdfs.close();
	}
	
	/**
	 * 
	 * @Description: 创建文件
	 * @param path
	 * @param text
	 * @throws Exception
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午10:04:55
	 *
	 */
	public static void createFileAndText(String path,String text)throws Exception{
		FileSystem hdfs = getFileSystem();
		FSDataOutputStream dos = hdfs.create(new Path(path));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
		bw.write(text);
		//bw.newLine();
		//bw.write("离线数据分析平台");
		bw.close();
		dos.close();
		hdfs.close();
	}
	
	
	
	/**
	 * 
	 * @Description: 读取文件
	 * @param path
	 * @throws Exception
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午10:13:24
	 *
	 */
	public static void readFile(String path)throws Exception{
		FileSystem hdfs = getFileSystem();
		InputStream is = hdfs.open(new Path(path));
		BufferedReader bReader = new BufferedReader(new InputStreamReader(is));
		String line =null ;
		while((line= bReader.readLine())!=null){
			System.out.println(line);		
		}
		bReader.close();
		is.close();
		hdfs.close();
	}
	
	/**
	 * 
	 * @Description: 删除文件
	 * @param path
	 * @param flag
	 * @throws Exception
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午10:19:05
	 *
	 */
	public static void deleteFile(String path ,boolean flag)throws Exception{
		FileSystem hdfs = getFileSystem();
		boolean b = hdfs.delete(new Path(path), flag);
		System.out.println("是否删除成功："+b);
		hdfs.close();
	}
	
	/**
	 * 
	 * @Description: 从本地拷贝文件到hdfs文件系统中
	 * @param src
	 * @param dst
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午10:23:46
	 *
	 */
	public static void CopyFromLocal(String src ,String dst)throws Exception{
		FileSystem hdfs = getFileSystem();
		hdfs.copyFromLocalFile(new Path(src), new Path(dst));
		hdfs.close();
	}
	
	/**
	 * 
	 * @Description: 拷贝hdfs文件到本地
	 * @param src
	 * @param dst
	 * @throws Exception
	 * @author wangxueqiang
	 * @date 2018年9月19日 上午10:27:50
	 *
	 */
	public  static void CopyToLocal(String src ,String dst)throws Exception{
		FileSystem hdfs = getFileSystem();
		hdfs.copyToLocalFile(new Path(src), new Path(dst));
		hdfs.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		//mkdirsFiles("/hdfs/java/api");
		//createFileAndText("/hdfs/java/api/1.txt", "大数据人工智能");
		readFile("/user/hadoop-data/1.txt");
	    //CopyToLocal("/hdfs/java/api/flume.txt", "D:/1.txt");
		//CopyFromLocal("D:/flume.txt", "/hdfs/java/api");
	}

}
