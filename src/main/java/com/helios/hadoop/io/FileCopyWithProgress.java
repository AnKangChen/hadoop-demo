package com.helios.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class FileCopyWithProgress {

    public static void main(String[] args) throws Exception {
        String src = args[0];
        String dest = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(src));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dest),conf);
        FSDataOutputStream out = fs.create(new Path(dest), () -> System.out.println("->"));
        IOUtils.copyBytes(in,out,4096,true);
    }
}
