package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings({"JavaDoc", "WeakerAccess"})
public class HdfsHelper {

    public static final Logger LOG = LoggerFactory.getLogger(HdfsWriter.Job.class);

    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    private Configuration writerConfig;
    public FileSystem fileSystem = null;
    public JobConf conf = null;
    public org.apache.hadoop.conf.Configuration hadoopConf = null;
    private UserGroupInformation ugi = null;

    // Kerberos
    private Boolean haveKerberos = false;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;

    public UserGroupInformation getUgi(){
        return ugi;
    }


    public void getFileSystem(String defaultFS, Configuration taskConfig) {
        this.writerConfig = taskConfig;
        //???????????????hadoop????????????
        this.hadoopConf = new org.apache.hadoop.conf.Configuration();
        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        this.hadoopConf.set("fs.defaultFS", defaultFS);

        //?????????Kerberos??????haveKerberos
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        HdfsUserGroupInfoLock.lock();
        try {
            if (haveKerberos) {
                System.setProperty("java.security.krb5.conf", taskConfig.getNecessaryValue(Key.KERBEROS_KRB5CONF_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE));
                this.kerberosKeytabFilePath = taskConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
                this.kerberosPrincipal = taskConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsWriterErrorCode.REQUIRED_VALUE);
                hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
                ugi = this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
            } else {
                ugi = getUgiInAuth(taskConfig);
            }

            try {
                fileSystem = ugi.doAs(new PrivilegedAction<FileSystem>() {
                    @Override
                    public FileSystem run() {
                        conf = new JobConf(hadoopConf);
                        FileSystem fs;
                        try {
                            fs = FileSystem.get(conf);
                        } catch (IOException e) {
                            String message = String.format("??????FileSystem???????????????IO??????,?????????????????????????????????!HDFS?????????[%s]", "message:defaultFS =" + defaultFS);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
                        } catch (Exception e) {
                            String message = String.format("??????FileSystem??????,?????????HDFS??????????????????: [%s]", "message:defaultFS =" + defaultFS);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
                        }
                        return fs;
                    }
                });
            } catch (Exception e) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        } finally {
            HdfsUserGroupInfoLock.unlock();
        }

        if (null == fileSystem || null == conf) {
            String message = String.format("??????FileSystem??????,?????????HDFS??????????????????: [%s]", "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    /**
     * kerberos ugi ????????????
     *
     * @param kerberosPrincipal      kerberos principal
     * @param kerberosKeytabFilePath kerberos keytab
     * @return UserGroupInformation
     */
    private UserGroupInformation kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        UserGroupInformation ugi = null;
        if (haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal.substring(0, kerberosPrincipal.indexOf("@")), kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos????????????,?????????kerberosKeytabFilePath[%s]???kerberosPrincipal[%s]????????????",
                        kerberosKeytabFilePath, kerberosPrincipal);
                throw DataXException.asDataXException(HdfsWriterErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }
        }
        return ugi;
    }

    /**
     * ???kerberos ugi ??????
     *
     * @param taskConfig
     * @return
     */
    private UserGroupInformation getUgiInAuth(Configuration taskConfig) {
        //????????????ldap??????
//        String userName = taskConfig.getString(Key.LDAP_USERNAME, "");
//        String password = taskConfig.getString(Key.LDAP_USERPASSWORD, "");
//        if(StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)){
//            try {
//                password = (String) CryptoUtils.string2Object(password);
//            } catch (Exception e) {
//                LOG.error("Fail to decrypt password", e);
//                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, e);
//            }
//            Properties properties = null;
//            try {
//                properties = LdapUtil.getLdapProperties();
//            }catch(Exception e){
//                //Ignore
//            }
//            if(null != properties){
//                LdapConnector ldapConnector = LdapConnector.getInstance(properties);
//                if(!ldapConnector.authenticate(userName, password)){
//                    throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, "LDAP authenticate fail");
//                }
//            }else{
//                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, "Engine need LDAP configuration");
//            }
//        }
        UserGroupInformation ugi;
        try {
            UserGroupInformation.setConfiguration(hadoopConf);
            String execUser = taskConfig.getString(Key.EXEC_USER, "");
            if (StringUtils.isNotBlank(execUser)) {
                ugi = UserGroupInformation.createRemoteUser(execUser);
            } else {
                ugi = UserGroupInformation.getCurrentUser();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_PROXY_ERROR, e);
        }
        return ugi;
    }


    /**
     * ????????????????????????????????????
     *
     * @param dir
     * @return ??????????????????????????????
     * eg???hdfs://10.101.204.12:9000/user/hive/warehouse/com.alibaba.datax.plugin.writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir) {
        Path path = new Path(dir);
        String[] files = null;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for (int i = 0; i < status.length; i++) {
                files[i] = status[i].getPath().toString();
            }
        } catch (IOException e) {
            String message = String.format("????????????[%s]???????????????????????????IO??????,????????????????????????????????????", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * ?????????fileName__ ?????????????????????
     *
     * @param dir
     * @param fileName
     * @return
     */
    public Path[] hdfsDirList(String dir, String fileName) {
        Path path = new Path(dir);
        Path[] files = null;
        String filterFileName = fileName + "__*";
        try {
            PathFilter pathFilter = new GlobFilter(filterFileName);
            FileStatus[] status = fileSystem.listStatus(path);
            files = new Path[status.length];
            for (int i = 0; i < status.length; i++) {
                files[i] = status[i].getPath();
            }
        } catch (IOException e) {
            String message = String.format("????????????[%s]???????????????[%s]????????????????????????????????????IO??????,????????????????????????????????????",
                    dir, fileName);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * DFS??????????????????????????????
     *
     * @param filePath
     * @return
     */
    public boolean isPathExists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String message = String.format("??????????????????[%s]???????????????????????????IO??????,????????????????????????????????????",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    /**
     * DFS ?????????????????????
     *
     * @param filePath
     * @return
     */
    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir = false;
        try {
            isDir = fileSystem.isDirectory(path);
        } catch (IOException e) {
            String message = String.format("????????????[%s]??????????????????????????????IO??????,????????????????????????????????????", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    /**
     * DFS ???????????????
     *
     * @param dirPath
     */
    public void createDir(String dirPath) {
        Path path = new Path(dirPath);
        try {
            if (!isPathExists(dirPath)) {
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            String message = String.format("????????????[%s]?????????IO??????,????????????????????????????????????", dirPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    /**
     * DFS ????????????
     *
     * @param paths
     */
    public void deleteFiles(Path[] paths) {
        for (int i = 0; i < paths.length; i++) {
            LOG.info(String.format("delete file [%s].", paths[i].toString()));
            try {
                fileSystem.delete(paths[i], true);
            } catch (IOException e) {
                String message = String.format("????????????[%s]?????????IO??????,????????????????????????????????????",
                        paths[i].toString());
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    /**
     * DFS ????????????
     *
     * @param path
     */
    public void deleteDir(Path path) {
        LOG.info(String.format("start delete tmp dir [%s] .", path.toString()));
        try {
            if (isPathExists(path.toString())) {
                fileSystem.delete(path, true);
            }
        } catch (Exception e) {
            String message = String.format("??????????????????[%s]?????????IO??????,????????????????????????????????????", path.toString());
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info(String.format("finish delete tmp dir [%s] .", path.toString()));
    }

    /**
     * ??????????????????
     *
     * @param tmpFiles
     * @param endFiles
     */
    @SuppressWarnings("rawtypes")
    public void renameFile(HashSet<String> tmpFiles, HashSet<String> endFiles) {
        Path tmpFilesParent = null;
        if (tmpFiles.size() != endFiles.size()) {
            String message = String.format("???????????????????????????????????????????????????????????????!");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
        } else {
            try {
                for (Iterator it1 = tmpFiles.iterator(), it2 = endFiles.iterator(); it1.hasNext() && it2.hasNext(); ) {
                    String srcFile = it1.next().toString();
                    String dstFile = it2.next().toString();
                    Path srcFilePah = new Path(srcFile);
                    Path dstFilePah = new Path(dstFile);
                    if (tmpFilesParent == null) {
                        tmpFilesParent = srcFilePah.getParent();
                    }
                    LOG.info(String.format("start rename file [%s] to file [%s].", srcFile, dstFile));
                    boolean renameTag = false;
                    long fileLen = fileSystem.getFileStatus(srcFilePah).getLen();
                    if (fileLen > 0) {
                        renameTag = fileSystem.rename(srcFilePah, dstFilePah);
                        if (!renameTag) {
                            String message = String.format("???????????????[%s]??????,????????????????????????????????????", srcFile);
                            LOG.error(message);
                            throw DataXException.asDataXException(HdfsWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
                        }
                        LOG.info(String.format("finish rename file [%s] to file [%s].", srcFile, dstFile));
                    } else {
                        LOG.info(String.format("?????????%s???????????????,??????????????????????????????", srcFile));
                    }
                }
            } catch (Exception e) {
                String message = String.format("??????????????????????????????,????????????????????????????????????");
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            } finally {
                deleteDir(tmpFilesParent);
            }
        }
    }

    /**
     * ??????file system
     */
    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("??????FileSystem?????????IO??????,????????????????????????????????????");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }


    /**
     * ??????????????????????????????FSDataOutputStream
     *
     * @param path
     * @return
     */
    public FSDataOutputStream getOutputStream(String path) {
        Path storePath = new Path(path);
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(storePath);
        } catch (IOException e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
        return fSDataOutputStream;
    }

    /**
     * ???textfile????????????
     *
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    @SuppressWarnings({"rawtypes", "static-access", "unchecked"})
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector) {
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_" + dateFormat.format(new Date()) + "_0001_m_000000_0";
        Path outputPath = new Path(fileName);
        //todo ?????????????????????TASK_ATTEMPT_ID
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        FileOutputFormat outFormat = new TextOutputFormat();
        outFormat.setOutputPath(conf, outputPath);
        outFormat.setWorkOutputPath(conf, outputPath);
        if (null != compress) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(), transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("???????????????[%s]?????????IO??????,????????????????????????????????????", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    /**
     * ??????record?????????????????????hdfs???????????????
     *
     * @param record
     * @param fieldDelimiter
     * @param columnsConfiguration
     * @param taskPluginCollector
     * @return
     */
    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
        MutablePair<List<Object>, Boolean> transportResultList = transportOneRecord(record, columnsConfiguration, taskPluginCollector);
        //??????<??????????????????,??????????????????>
        MutablePair<Text, Boolean> transportResult = new MutablePair<Text, Boolean>();
        transportResult.setRight(false);
        if (null != transportResultList) {
            Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
            transportResult.setRight(transportResultList.getRight());
            transportResult.setLeft(recordResult);
        }
        return transportResult;
    }

    /**
     * ?????????????????????????????????
     *
     * @param compress
     * @return
     */
    public Class<? extends CompressionCodec> getCompressCodec(String compress) {
        Class<? extends CompressionCodec> codecClass = null;
        if (null == compress) {
            codecClass = null;
        } else if ("GZIP".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        } else if ("BZIP2".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        } else if ("SNAPPY".equalsIgnoreCase(compress)) {
            //todo ???????????????????????? ??????????????????SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
            // org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class  not public
            //codecClass = org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class;
        } else {
            throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                    String.format("??????????????????????????? compress ?????? : [%s]", compress));
        }
        return codecClass;
    }

    /**
     * ???orcfile????????????
     *
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    @SuppressWarnings({"rawtypes", "static-access", "unchecked"})
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector) {
        List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);
        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StructObjectInspector inspector = (StructObjectInspector) ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypeInspectors);

        OrcSerde orcSerde = new OrcSerde();

        FileOutputFormat outFormat = new OrcOutputFormat();
        if (!"NONE".equalsIgnoreCase(compress) && null != compress) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        //optimize orc write speed avoid gc over limited exceed problem by orc record com.alibaba.datax.plugin.writer local cache
        conf.set("hive.exec.orc.default.stripe.size", "33554432"); //67108864(default)->33554432
        conf.set("hive.exec.orc.default.buffer.size", "16384"); //262144(default)->16384
        //conf.set("hive.exec.orc.skip.corrupt.data","true"); //false(default)->true

        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<List<Object>, Boolean> transportResult = transportOneRecord(record, columns, taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(), orcSerde.serialize(transportResult.getLeft(), inspector));
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("???????????????[%s]?????????IO??????,????????????????????????????????????", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    /**
     * ????????????????????????????????????
     *
     * @param columns
     * @return
     */
    public List<String> getColumnNames(List<Configuration> columns) {
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }

    /**
     * ??????writer??????????????????????????????inspector
     *
     * @param columns
     * @return
     */
    public List<ObjectInspector> getColumnTypeInspectors(List<Configuration> columns) {
        List<ObjectInspector> columnTypeInspectors = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            ObjectInspector objectInspector = null;
            switch (columnType) {
                case TINYINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case SMALLINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case INT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BIGINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case FLOAT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DOUBLE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case TIMESTAMP:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DATE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BOOLEAN:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "?????????????????????????????????????????????. ??????DataX ??????????????????????????????????????????. ?????????:[%s], ????????????:[%d]. ?????????????????????????????????????????????????????????.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }

            columnTypeInspectors.add(objectInspector);
        }
        return columnTypeInspectors;
    }

    /**
     * ??????orc serder
     *
     * @param config
     * @return
     */
    public OrcSerde getOrcSerde(Configuration config) {
        String fieldDelimiter = config.getString(Key.FIELD_DELIMITER);
        String compress = config.getString(Key.COMPRESS);
        String encoding = config.getString(Key.ENCODING);

        OrcSerde orcSerde = new OrcSerde();
        Properties properties = new Properties();
        properties.setProperty("orc.bloom.filter.columns", fieldDelimiter);
        properties.setProperty("orc.compress", compress);
        properties.setProperty("orc.encoding.strategy", encoding);

        orcSerde.initialize(conf, properties);
        return orcSerde;
    }

    /**
     * ??????record?????????????????????hdfs???????????????
     *
     * @param record
     * @param columnsConfiguration
     * @param taskPluginCollector
     * @return
     */
    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record, List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector) {

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<List<Object>, Boolean>();
        transportResult.setRight(false);
        List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                //todo as method
                if (null != column.getRawData()) {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase());
                    //??????writer??????????????????????????????
                    try {
                        switch (columnType) {
                            case TINYINT:
                                recordList.add(Byte.valueOf(rowData));
                                break;
                            case SMALLINT:
                                recordList.add(Short.valueOf(rowData));
                                break;
                            case INT:
                                recordList.add(Integer.valueOf(rowData));
                                break;
                            case BIGINT:
                                recordList.add(column.asLong());
                                break;
                            case FLOAT:
                                recordList.add(Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                recordList.add(column.asDouble());
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                recordList.add(column.asString());
                                break;
                            case BOOLEAN:
                                recordList.add(column.asBoolean());
                                break;
                            case DATE:
                                recordList.add(new java.sql.Date(column.asDate().getTime()));
                                break;
                            case TIMESTAMP:
                                recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "?????????????????????????????????????????????. ??????DataX ??????????????????????????????????????????. ?????????:[%s], ????????????:[%d]. ?????????????????????????????????????????????????????????.",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: ?????????????????????
                        String message = String.format(
                                "?????????????????????????????????????????????[%s]???????????????????????????[%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData().toString());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                } else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }


    /**
     * ??????????????????
     *
     * @param path
     * @param content
     */
    public void addSucessFile(String path, String content) {
        if (isPathExists(path)) {
            Path dfsPath = new Path(path);
            deleteDir(dfsPath);
        }
        try {
            FSDataOutputStream outputStream = getOutputStream(path);
            byte[] buff = content.getBytes();
            outputStream.write(buff, 0, buff.length);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            String message = String.format("Create an successFile at the indicated Path[%s] failed: [%s]", "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }


}
