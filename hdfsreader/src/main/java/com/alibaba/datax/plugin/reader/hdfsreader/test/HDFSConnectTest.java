package com.alibaba.datax.plugin.reader.hdfsreader.test;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.hdfsreader.HdfsReaderErrorCode;
import com.alibaba.datax.plugin.reader.hdfsreader.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HDFSConnectTest {

    final String username = "hdfsUser";

    @SuppressWarnings("unused")
    public static void main(String[] args) {
        HDFSConnectTest hdfsConnectTest = new HDFSConnectTest();
        //hdfsConnectTest.connectSimpleAuthCluster();
        hdfsConnectTest.connectKerberosAuthCluster();
    }

    private void connectSimpleAuthCluster() {
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\bin");
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    FileSystem fs = null;
                    try {
                        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                        conf.set("fs.defaultFS", "hdfs://172.25.224.193:8020");
                        conf.set("hadoop.security.authentication", "simple");
                        conf.set("hadoop.job.ugi", username);
                        fs = FileSystem.get(conf);
                        //只读当前子路径
//                        FileStatus[] fileStatus = fs.listStatus(new Path("/user/hive/warehouse/tmp.db"));
//                        Path[] listPath = FileUtil.stat2Paths(fileStatus);
//                        for (Path path : listPath) {
//                            System.out.println(String.format("current child path:%s", path.toString()));
//                        }

                        Path path = new Path("/user/hive/warehouse/tmp.db/test2");
                        fs.delete(path);
                        fs.mkdirs(path);
                        FileStatus fileStatus = fs.getFileStatus(path);
                        System.out.println(String.format("acl status:%s", fileStatus.toString()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            fs.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void connectKerberosAuthCluster() {
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\bin");

        final Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://172.20.216.158:8020");
        hadoopConf.set("hadoop.security.authentication", "kerberos");

        System.setProperty("java.security.krb5.conf", "D:\\hadoop\\keytab\\krb5.conf");
        //System.setProperty("sun.security.krb5.debug", "true");
        String kerberosPrincipal = "zhongan_test@DATA-CDH-D1.COM";
        String kerberosKeytabFilePath = "D:\\hadoop\\keytab\\zhongan_test.keytab";

        UserGroupInformation ugi  = this.kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
        try {
            FileSystem fileSystem =  ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    FileSystem fs = null;
                    try {
                        fs = FileSystem.get(hadoopConf);
                        //只读当前子路径

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return fs;
                }
            });

            try{
                FileStatus[] fileStatusArray = fileSystem.listStatus(new Path("/user/datafarm/zhongan_test/ods_zl_task_demo/20210315/"));
                for (FileStatus fileStatus : fileStatusArray) {
                    System.out.println(String.format("current child path:%s", fileStatus.getLen()));
                }
            }
            finally {
                try {
                    if (fileSystem != null){
                        fileSystem.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private UserGroupInformation kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, Configuration hadoopConf) {
        UserGroupInformation ugi = null;
        if (StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal
                        .substring(0, kerberosPrincipal.indexOf("@")), kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                throw DataXException.asDataXException(HdfsReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }

        }
        return ugi;
    }

}
