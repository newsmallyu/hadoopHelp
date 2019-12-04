package com.newegg.hadoopHelp.service;

import com.newegg.hadoopHelp.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HService {

    public String exist(String user,String fsDefaultName,String path){
        System.setProperty("HADOOP_USER_NAME", user);
        Configuration conf = new Configuration();
        conf.set("fs.default.name",fsDefaultName);
        String result = null;
        try {
            result = HDFSUtil.exist(path, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void mkdir(String user,String fsDefaultName,String path) throws IOException {
        System.setProperty("HADOOP_USER_NAME", user);
        Configuration conf = new Configuration();
        conf.set("fs.default.name",fsDefaultName);
        HDFSUtil.mkdir(path, conf);
    }


    public void uploadFile(String user,String fsDefaultName,String srcpath,String dstpath) throws IOException {
        System.setProperty("HADOOP_USER_NAME", user);
        Configuration conf = new Configuration();
        conf.set("fs.default.name",fsDefaultName);
        HDFSUtil.uploadFile(srcpath,dstpath,conf);
    }

    public void delete (String user,String fsDefaultName,String path) throws IOException {
        System.setProperty("HADOOP_USER_NAME", user);
        Configuration conf = new Configuration();
        conf.set("fs.default.name",fsDefaultName);
        HDFSUtil.delete(path,conf);
    }

    public void download(String user,String fsDefaultName,String srcpath,String dstpath) throws IOException {
        System.setProperty("HADOOP_USER_NAME", user);
        Configuration conf = new Configuration();
        conf.set("fs.default.name",fsDefaultName);
        HDFSUtil.downloadFile(srcpath,dstpath,conf);
    }
}
