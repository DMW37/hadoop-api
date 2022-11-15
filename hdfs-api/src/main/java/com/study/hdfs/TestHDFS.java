package com.study.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description: 测试HDFS上传下载
 */
public class TestHDFS {
    // 定义文件系统
    FileSystem fileSystem = null;

    @Before
    public void before() throws IOException {
        Configuration configuration = new Configuration(true);
        fileSystem = FileSystem.get(configuration);
    }

    /**
     * 文件上传
     * @throws IOException
     */
    @Test
    public void testFileUpload() throws IOException {
        // 定义资源路径
        Path srcPath = new Path("D:\\application.yml");
        // 定义目标路径
        Path destPath = new Path("/msb/application.yml");
        fileSystem.copyFromLocalFile(srcPath,destPath);
    }

    /**
     * 文件下载
     * @throws IOException
     */
    @Test
    public void testDownload() throws IOException {
        // 定义资源路径
        Path srcPath = new Path("/msb/application.yml");
        // 定义目标路径
        Path destPath = new Path("D:\\a.yml");
        fileSystem.copyToLocalFile(srcPath,destPath);
    }
}
