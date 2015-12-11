import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    Map<String, Integer> resultMap;

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

        resultMap = new HashMap<>();

    }


    public void readFile(String file) throws IOException {

        Path path = new Path(file);

        logger.info("Filesystem URI : " + fileSystem.getUri());
        logger.info("Filesystem Home Directory : " + fileSystem.getHomeDirectory());
        logger.info("Filesystem Working Directory : " + fileSystem.getWorkingDirectory());
        logger.info("HDFS File Path : " + path);

        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
                file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
                new File(filename)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }
    public void addFile(String dest) throws IOException {


// Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

// Create a new file and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );

        resultMap.entrySet().stream()
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
// Close all the file descripters
        br.close();
        out.close();
        fileSystem.close();
    }

    public void dirContent(String directory) {
        try{
            FileStatus[] status = fileSystem.listStatus(new Path(directory));  // you need to pass in your hdfs path

            for (int i = 0; i < status.length; i++) {
                logger.info(status[i].getPath().toString());

            }
        } catch (Exception e) {
            System.out.println("File not found");
        }
    }

    public void processDirectory(String directory) {
        try {
            FileStatus[] status = fileSystem.listStatus(new Path(directory));  // you need to pass in your hdfs path

            for (int i=0;i<status.length;i++){
                if (status[i].getPath().toString().compareTo("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130612.txt") != 0)
                    continue;

                BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    processLine(line);
                    line=br.readLine();
                }
            }

        }catch(Exception e){
            System.out.println("File not found");
        }
    }

    public void processLine(String line){
        String[] splittedLine = line.split("\t", 4);

        //logger.debug( "1=[{}] 2=[{}] 3=[{}] 4=[{}] ", splittedLine[0], splittedLine[1], splittedLine[2], splittedLine[3]);
        String id = splittedLine[2];


        Integer count = resultMap.get(id);
        if (count == null)
            resultMap.put(id, 1);
        else
            resultMap.put(id, count+1);
    }

    public static void main(String[] args) throws IOException {


        HDFSClient client = new HDFSClient();
        client.readFile("ipinyou.contest.dataset/bid.20130612.txt");

    }

    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue( Map<K, V> map )
    {
        Map<K,V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();

        st.sorted(Comparator.comparing(e -> e.getValue()))
                .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        return result;
    }
}