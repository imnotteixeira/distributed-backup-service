import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.Network;

import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class Main {

    public static void main(String[] args) {
        // Necessary for MacOS operating system to prioritize IPv4 interfaces
        System.setProperty("java.net.preferIPv4Stack", "true");
        try {

            int port = Integer.valueOf(args[0]);
            InetAddress ip = Network.getSelfAddress(port);

            NodeInfo selfInfo = new NodeInfo(ip, port);

            ConsoleLogger.bootstrap(new SimpleNodeInfo(selfInfo));
            ConsoleLogger.log(Level.INFO, "Started Node.");


            Node n;
            if(args.length < 3) {
                n = new Node(selfInfo);
            } else {
                InetAddress SUCCESSOR_IP = InetAddress.getByName(args[1]);
                int SUCCESSOR_PORT = Integer.valueOf(args[2]);


                NodeInfo successorInfo = new NodeInfo(SUCCESSOR_IP, SUCCESSOR_PORT);
                n = new Node(selfInfo, successorInfo);
            }


        } catch (ExecutionException | InterruptedException | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }


    }
}
