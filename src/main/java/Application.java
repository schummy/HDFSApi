import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by user on 12/8/15.
 */
public class Application {
    public static void main(String[] args){

        try {
            HDFSClient hdfsClient = new HDFSClient();
            hdfsClient.dirContent("ipinyou.contest.dataset");

            hdfsClient.processDirectory("ipinyou.contest.dataset");
            hdfsClient.addFile("ipinyou.contest.dataset/bid_result.txt");
            //hdfsClient.readFile("ipinyou.contest.dataset/bid.20130612.txt");

            //hdfsClient.getHostnames();
        } catch (IOException e) {
            e.printStackTrace();

        }
    }
}
