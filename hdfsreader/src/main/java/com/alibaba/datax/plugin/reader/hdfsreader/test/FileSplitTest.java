package com.alibaba.datax.plugin.reader.hdfsreader.test;

import java.util.ArrayList;
import java.util.List;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class FileSplitTest {

    public static void main(String[] args) {

//        testHdfsFileMoreThanAdviceNumber();
//        testHdfsFileEqualAdviceNumber();
//        testHdfsFileLessThanAdviceNumber();
        testEmptyHdfsFileLessThanAdviceNumber();
    }


    private static void testHdfsFileMoreThanAdviceNumber() {
        List<String> list = new ArrayList<String>();
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");


        List<List<String>> splitSourceFiles = splitSourceFiles(list, 2);
        System.out.println(String.format("split job with fileS..[%s]", JSON.toJSONString(splitSourceFiles, SerializerFeature.PrettyFormat)));
    }


    private static void testHdfsFileEqualAdviceNumber() {
        List<String> list = new ArrayList<String>();
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");


        List<List<String>> splitSourceFiles = splitSourceFiles(list, 6);
        System.out.println(String.format("split job with fileS..[%s]", JSON.toJSONString(splitSourceFiles, SerializerFeature.PrettyFormat)));
    }


    private static void testHdfsFileLessThanAdviceNumber() {
        List<String> list = new ArrayList<String>();
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");


        List<List<String>> splitSourceFiles = splitSourceFiles(list, 10);
        System.out.println(String.format("split job with fileS..[%s]", JSON.toJSONString(splitSourceFiles, SerializerFeature.PrettyFormat)));
    }


    private static void testEmptyHdfsFileLessThanAdviceNumber() {
        List<String> list = new ArrayList<String>();
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0 ");
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy1");
//        list.add("/user/hive/warehouse/xman.db/ods_sdk_event_record_cxf/dt=20200902/000000_0_copy2");


        List<List<String>> splitSourceFiles = splitSourceFiles(list, 1);
        System.out.println(String.format("split job with fileS..[%s]", JSON.toJSONString(splitSourceFiles, SerializerFeature.PrettyFormat)));
    }



//    private static List<List<String>> splitSourceFiles(List<String> sourceList, int adviceNumber) {
//        List<List<String>> splitedList = new ArrayList<List<String>>();
//        if (sourceList.size() >=  adviceNumber) {
//            int averageLength = sourceList.size() / adviceNumber;
//            averageLength = averageLength == 0 ? 1 : averageLength;
//            int modNumber = sourceList.size() % adviceNumber;
//            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
//                end = begin + averageLength + modNumber;
//                if (end > sourceList.size()) {
//                    end = sourceList.size();
//                }
//                splitedList.add(sourceList.subList(begin, end));
//            }
//        }else{
//            for (String filePath : sourceList) {
//                List<String> filePathList = new ArrayList<String>();
//                filePathList.add(filePath);
//                splitedList.add(filePathList);
//            }
//
//            int diff = adviceNumber - sourceList.size();
//            for (int i = 0; i < diff; i ++){
//                splitedList.add(new ArrayList<String>());
//            }
//
//        }
//        return splitedList;
//    }


    //添加取余处理
    private static List<List<String>> splitSourceFiles(List<String> sourceList, int adviceNumber) {
        List<List<String>> splitedList = new ArrayList<List<String>>();
        int averageLength = sourceList.size() / adviceNumber;
        averageLength = averageLength == 0 ? 1 : averageLength;
        int modNumber = sourceList.size() % adviceNumber;
        for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
            end = begin + averageLength + modNumber;
            if (end > sourceList.size()) {
                end = sourceList.size();
            }
            splitedList.add(sourceList.subList(begin, end));
        }
        return splitedList;
    }

}
