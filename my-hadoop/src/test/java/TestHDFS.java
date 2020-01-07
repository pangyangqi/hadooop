import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

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
}
