import com.dbs.protocols.IDistributedBackupService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class TestApp {

    private static IDistributedBackupService peer(String ap) throws RemoteException, NotBoundException {
        return (IDistributedBackupService) LocateRegistry.getRegistry("localhost").lookup(ap);
    }

    public static void main(String[] args) {
        try {
            IDistributedBackupService backupService = TestApp.peer(args[0]);

            switch (args[1]) {
                case "BACKUP":
                    if(args.length != 4){
                        System.out.println("No file was provided. Usage: <PeerAP> BACKUP <fileName> <repDegree>");
                    }else {
                        int repDegree = Integer.parseInt(args[3]);
                        System.out.println("Backing up file " + args[2] + " with replication degree of " +  repDegree + "...\n");
                        System.out.println(backupService.backup(args[2], repDegree));
                    }
                    break;
                case "RESTORE":
                    if(args.length != 3){
                        System.out.println("No file was provided. Usage: <PeerAP> RESTORE <fileName>");
                    }else {
                        System.out.println("Restoring file " + args[2] + "...\n");
                        System.out.println(backupService.restore(args[2]));
                    }
                    break;
                case "DELETE":
                    if(args.length != 3){
                        System.out.println("No file was provided. Usage: <PeerAP> DELETE <fileName>");
                    }else {
                        System.out.println("Deleting file " + args[2] + "...\n");
                        System.out.println(backupService.delete(args[2]));
                    }
                    break;
                case "STATE":
                    System.out.println(backupService.state());
                    break;
                case "RECLAIM":
                    if(args.length != 3){
                        System.out.println("No number of bytes were provided. Usage: <PeerAP> RECLAIM <newSizeBytes>");
                    }else {
                        System.out.println("Reclaiming space: " + args[2] + "bytes...\n");
                        System.out.println(backupService.reclaim(Integer.parseInt(args[2])));
                    }
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
