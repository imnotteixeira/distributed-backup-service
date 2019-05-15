import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.utils.Network;

import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello!");

        try {


            int port = Integer.valueOf(args[0]);
            InetAddress ip = Network.getSelfAddress(port);

            NodeInfo selfInfo = new NodeInfo(ip, port);


            Node n;
            if(args.length < 3) {
                n = new Node(selfInfo);
            } else {
                InetAddress SUCCESSOR_IP = InetAddress.getByName(args[1]);
                int SUCCESSOR_PORT = Integer.valueOf(args[2]);


                NodeInfo successorInfo = new NodeInfo(SUCCESSOR_IP, SUCCESSOR_PORT);
                n = new Node(selfInfo, successorInfo);
            }


            System.out.println(n);






        } catch (ExecutionException | InterruptedException | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }


    }
}
