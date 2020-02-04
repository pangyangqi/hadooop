import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * @author Yangqi.Pang
 * @date 2020-01-07 07:56
 */
public class TestHDFS {
    @Test
    public void testPut() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s102:8020/user/centos/2.txt");
        FSDataOutputStream out = fs.create(path);
        out.write("hello world".getBytes());
        out.flush();
        out.close();
    }


    @Test
    public void testPut2() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s102:8020/user/centos/1.txt");
        FSDataOutputStream out = fs.create(path,true,1024,(short)2,512);
        out.write("hello world".getBytes());
        out.flush();
        out.close();
    }

    @Test
    public void testRead() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s102:8020/user/centos/2.txt");
        FSDataInputStream in = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(in,baos,1024);
        System.out.println(new String(baos.toByteArray()));
    }
}
