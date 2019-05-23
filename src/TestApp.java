import com.dbs.backup.BackupService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class TestApp {

    private static BackupService lookup(String ap) throws RemoteException, NotBoundException {
        return (BackupService) LocateRegistry.getRegistry("localhost").lookup(ap);
    }

    public static void main(String[] args) {
        try {
            BackupService peer = TestApp.lookup(args[0]);
            switch (args[1]) {
                case "BACKUP":
                    System.out.println(peer.backup("asd.asd"));
                    break;
                case "RESTORE":
                    System.out.println(peer.restore("file"));
                    break;
                case "DELETE":
                    System.out.println(peer.delete("file"));
                    break;
                case "STATE":
                    System.out.println(peer.state());
                    break;
                default:
                    throw new InvalidSubprotocolException();
            }
        } catch (InvalidSubprotocolException e) {
            System.out.println("Invalid protocol");
        } catch (RemoteException e) {
            System.out.println("Failed to execute RMI call");
        } catch (NotBoundException e) {
            System.out.println("Peer \'" + args[0] + "\' not bound in RMI registry");
        }
    }

    private static class InvalidSubprotocolException extends Exception {
        InvalidSubprotocolException() {
            super("Invalid protocol");
        }
    }


}
