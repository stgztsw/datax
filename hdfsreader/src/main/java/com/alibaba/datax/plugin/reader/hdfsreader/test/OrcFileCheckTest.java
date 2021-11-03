package com.alibaba.datax.plugin.reader.hdfsreader.test;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OrcFileCheckTest {
    private static final Logger LOG = LoggerFactory.getLogger(OrcFileCheckTest.class);
    private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "D:\\hadoop\\bin");
        System.setProperty("java.security.krb5.conf", "D:\\hadoop\\keytab\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "false");


        String dfsPath = "/user/hive/warehouse/zhongan_test.db/r_ci_bal_d/date_id_zadw=20210129";
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://172.20.216.158:8020");
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);

        try {
            UserGroupInformation.loginUserFromKeytab("hive/data-cdh-d1-03@DATA-CDH-D1.COM", "D:\\hadoop\\keytab\\hive.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
            //只读当前子路径
            FileStatus[] fileStatus = fs.listStatus(new Path(dfsPath));
            Path[] listPath = FileUtil.stat2Paths(fileStatus);
            for (Path path : listPath) {
                System.out.println(String.format("current child path:%s", path.toString()));
                FSDataInputStream in = fs.open(path);
                System.out.println(isORCFile(path, fs, in));
            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    // 判断file是否是ORC File
    private static boolean isORCFile(Path file, FileSystem fs, FSDataInputStream in) {
        try {
            // figure out the size of the file using the option or filesystem
            long size = fs.getFileStatus(file).getLen();

            //read last bytes into buffer to get PostScript
            int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.get(readSize - 1) & 0xff;
            int len = OrcFile.MAGIC.length();
            if (psLen < len + 1) {
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1
                    - len;
            byte[] array = buffer.array();
            // now look for the magic string at the end of the postscript.
            if (Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
                return true;
            } else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                if (Text.decode(header, 0, len).equals(OrcFile.MAGIC)) {
                    return true;
                }
            }
        } catch (IOException e) {
            LOG.info(String.format("检查文件类型: [%s] 不是ORC File.", file.toString()));
        }
        return false;
    }


}
