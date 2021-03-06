import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codepoetics.protonpack.maps.MapStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * Created by user on 12/8/15.
 */
public class HDFSClient {

    final static String HADOOP_CONF_PATH = "/Users/user/bigData/hadoop-2.7.1/etc/hadoop/";
    FileSystem fileSystem;
    Configuration conf;

    public static void setLogger(Logger logger) {
        HDFSClient.logger = logger;
    }

    private static Logger logger;

    public HDFSClient() throws IOException {
        conf = new Configuration();
        conf.addResource(new Path(HADOOP_CONF_PATH + "core-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_PATH + "hdfs-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_PATH + "mapred-site.xml"));
        fileSystem = FileSystem.get(conf);
        setLogger(LoggerFactory.getLogger(HDFSClient.class));

        logger.info("HDFSClient()");
        logger.info(fileSystem.toString());

    }



    public void processDirectory(String directory) {
        try {
            Map<String, Integer>  outMap = new HashMap<>();

            FileStatus[] status = fileSystem.listStatus(new Path(directory));  // you need to pass in your hdfs path

            for ( int i = 0; i < status.length; i++ ){
                processFile(outMap, status[i]);
            }

            Path path = initOutputDir(directory);
            FSDataOutputStream out = fileSystem.create(path);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
            processMap(outMap, br);
// Close all the file descripters
            br.close();
            out.close();

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void processMap(Map<String, Integer> outMap, BufferedWriter br) {
        outMap.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (x, y) -> {
                        throw new AssertionError();
                    },
                    LinkedHashMap::new
            ))
            .forEach((k, v) -> {
                try {
                    br.write(k + "\t" + v + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
    }

    private Path initOutputDir(String directory) throws IOException {
        Path path = new Path(directory+"/bid_result.txt");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        return path;
    }

    private void processFile(Map<String, Integer> outMap, FileStatus statu) throws IOException {
        long startTime = System.nanoTime();

        logger.info("Processing {} file", statu.getPath().toString());
        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(statu.getPath())));
        String line = br.readLine();
        while (line != null){

            String[] splittedLine = line.split("\t", 4);
   //         String id1 = line.split("\t", 4)[2];

            String id = splittedLine[2];
            outMap.merge(id, 1, Integer::sum);
            line=br.readLine();
        }
        long elapsedTime = System.nanoTime() - startTime;

        logger.info("FIle {} processed in {} sec.", statu.getPath().toString(), elapsedTime/1000000000);
        logger.info("outMap has {} elements", outMap.size());
    }

}