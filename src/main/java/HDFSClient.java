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
    Map <String, Map<String, Integer> > resultMap;
    List<String> listOfIDs;

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

        resultMap = new TreeMap<>();
        listOfIDs = new LinkedList<>();

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
       /* resultMap = listOfIDs.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        */

/*
        Map<String, Integer> mx = Stream.of(m1, m2)
                .map(Map::entrySet)          // converts each map into an entry set
                .flatMap(Collection::stream) // converts each set into an entry stream, then
                        // "concatenates" it in place of the original set
                .collect(
                        Collectors.toMap(        // collects into a map
                                Map.Entry::getKey,   // where each entry is based
                                Map.Entry::getValue, // on the entries in the stream
                                Integer::max         // such that if a value already exist for
                                // a given key, the max of the old
                                // and new value is taken
                        )
                )
                ;
                */
            Map<String, Integer> mx = resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130606.txt");
             mx = MapStream.ofMaps(
                    mx,
                    resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130607.txt"))
                    .mergeKeys(Integer::sum).collect();

        mx = MapStream.ofMaps(
                mx,
                resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130608.txt"))
                .mergeKeys(Integer::sum).collect();
        mx = MapStream.ofMaps(
                mx,
                resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130609.txt"))
                .mergeKeys(Integer::sum).collect();
        mx = MapStream.ofMaps(
                mx,
                resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130607.txt"))
                .mergeKeys(Integer::sum).collect();
        mx = MapStream.ofMaps(
                mx,
                resultMap.get("hdfs://localhost:9000/user/user/ipinyou.contest.dataset/bid.20130607.txt"))
                .mergeKeys(Integer::sum).collect();

       /* resultMap.entrySet().stream()
               // .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
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
                });*/
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
            Map<String, Integer>  outMap = new HashMap<>();

            FileStatus[] status = fileSystem.listStatus(new Path(directory));  // you need to pass in your hdfs path

            for (int i=0;i<status.length;i++){
                long startTime = System.nanoTime();

                logger.info("Processing {} file", status[i].getPath().toString());
                //Map<String, Integer> fileMap = new HashMap<>();
                BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(status[i].getPath())));
                String line = br.readLine();
                while (line != null){

                    String[] splittedLine = line.split("\t", 4);

                    String id = splittedLine[2];
                    //fileMap.merge(id, 1, Integer::sum);
                    outMap.merge(id, 1, Integer::sum);
                   /* count = fileMap.get(id);
                    if (count == null) {
                        //fileMap.put(id,  new AtomicInteger(1) );
                        fileMap.put(id,  1 );
                    }
                    else {
                        //count.incrementAndGet();
                        fileMap.put(id, count + 1);
                    }*/
                    line=br.readLine();
                }
                //resultMap.put(status[i].getPath().toString(), fileMap);
                long elapsedTime = System.nanoTime() - startTime;

                logger.info("FIle {} processed in {} ns.", status[i].getPath().toString(), elapsedTime);

            }
            Path path = new Path(directory+"/bid_result.txt");
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
            FSDataOutputStream out = fileSystem.create(path);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
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
// Close all the file descripters
            br.close();
            out.close();

        }catch(Exception e){
            e.printStackTrace();
        }
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