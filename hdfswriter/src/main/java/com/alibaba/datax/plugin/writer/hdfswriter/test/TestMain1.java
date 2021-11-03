package com.alibaba.datax.plugin.writer.hdfswriter.test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Set;

public class TestMain1 {



    public void getFileSystem(String defaultFS) {

//        Configuration hadoopConf = new Configuration();
//
//        //io.file.buffer.size 性能参数
//        //http://blog.csdn.net/yangjl38/article/details/7583374
//        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
//        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
//        if (null != hadoopSiteParams) {
//            Set<String> paramKeys = hadoopSiteParams.getKeys();
//            for (String each : paramKeys) {
//                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
//            }
//        }
//        hadoopConf.set("fs.defaultFS", defaultFS);
//
//        //是否有Kerberos认证haveKerberos
//        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
//        if (haveKerberos) {
//            this.kerberosKeytabFilePath = taskConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
//            this.kerberosPrincipal = taskConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsWriterErrorCode.REQUIRED_VALUE);
//            System.setProperty("java.security.krb5.conf", taskConfig.getNecessaryValue(Key.KERBEROS_KRB5CONF_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE));
//            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
//        }
//        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
//
//        conf = new JobConf(hadoopConf);
//        try {
//            fileSystem = FileSystem.get(conf);
//        } catch (IOException e) {
//            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
//                    "message:defaultFS =" + defaultFS);
//            LOG.error(message);
//            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        } catch (Exception e) {
//            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
//                    "message:defaultFS =" + defaultFS);
//            LOG.error(message);
//            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        }
//
//        if (null == fileSystem || null == conf) {
//            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
//                    "message:defaultFS =" + defaultFS);
//            LOG.error(message);
//            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
//        }
    }

}


