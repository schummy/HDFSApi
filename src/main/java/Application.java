import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by user on 12/8/15.
 */
public class Application {
    public static void main(String[] args){
        long startTime = System.nanoTime();
        Logger logger =  LoggerFactory.getLogger(HDFSClient.class);

        try {
            HDFSClient hdfsClient = new HDFSClient();
            //hdfsClient.dirContent("ipinyou.contest.dataset");

            hdfsClient.processDirectory("ipinyou.contest.dataset");
            //hdfsClient.addFile("ipinyou.contest.dataset/bid_result.txt");

        } catch (IOException e) {
            e.printStackTrace();

        }
        long elapsedTime = System.nanoTime() - startTime;
        logger.info("Time spent {} seconds", elapsedTime/1000000000);
    }
}
