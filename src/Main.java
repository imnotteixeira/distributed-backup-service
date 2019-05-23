import com.dbs.chord.Chord;
import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.filemanager.FileManager;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.Network;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class Main {

    public static void main(String[] args) {
        // Necessary for MacOS operating system to prioritize IPv4 interfaces
        System.setProperty("java.net.preferIPv4Stack", "true");


        try {
            String accessPoint = args[0];

            int port = Integer.valueOf(args[1]);
            InetAddress ip = Network.getSelfAddress(port);

            NodeInfo selfInfo = new NodeInfo(ip, port);
            selfInfo.setAccessPoint(accessPoint);

            ConsoleLogger.bootstrap(new SimpleNodeInfo(selfInfo));
            ConsoleLogger.log(Level.INFO, "Started Node.");


            Node n;
            if(args.length < 4) {
                n = new Node(selfInfo);
            } else {
                InetAddress SUCCESSOR_IP = InetAddress.getByName(args[2]);
                int SUCCESSOR_PORT = Integer.valueOf(args[3]);


                NodeInfo successorInfo = new NodeInfo(SUCCESSOR_IP, SUCCESSOR_PORT);
                n = new Node(selfInfo, successorInfo);
            }


        } catch (ExecutionException | InterruptedException | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }

//        try {
//
//            Path folder = FileManager.createDirectory("test");
//
//            byte[] data = FileManager.readFromFile("asd.asd");
//
//            FileManager.writeToFile(Paths.get(folder.toString(), "filecopy").toString(), data);
//
//            System.out.println(new String(data, 0, data.length));
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }
}
