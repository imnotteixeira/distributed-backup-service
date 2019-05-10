import com.dbs.chord.Node;
import com.dbs.utils.Network;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello!");
        try {

            int port = 1234;
            InetAddress ip = Network.getSelfAddress(port);

            System.out.println("My IP is " + ip);

            Node n = new Node(ip, port);
            System.out.println(n);






        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
