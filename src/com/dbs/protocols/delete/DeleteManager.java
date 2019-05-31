package com.dbs.protocols.delete;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.Utils;
import com.dbs.filemanager.FileManager;
import com.dbs.network.messages.ChordMessage;
import com.dbs.network.messages.DeleteConfirmationMessage;
import com.dbs.network.messages.DeleteReplicaMessage;
import com.dbs.network.messages.NotFoundMessage;
import com.dbs.protocols.backup.FileIdentifier;
import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.utils.Network;

import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.dbs.chord.Node.REQUEST_TIMEOUT_MS;

public class DeleteManager {

    private final Node node;

    public DeleteManager(Node node) {
        this.node = node;

    }

    public String delete(String file) throws RemoteException {

        StringBuilder retMsg = new StringBuilder();

        try {
            FileIdentifier fileId = FileIdentifier.fromPath(file);


            if (this.node.getBackupManager().getDesiredFileRepDegreeOfFile(fileId) == null) {
                return "File is not backed up";
            }

            ReplicaIdentifier[] replicaIds = FileManager.generateReplicaIds(fileId, this.node.getBackupManager().getDesiredFileRepDegreeOfFile(fileId));

            ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>();

            for(ReplicaIdentifier replicaId : replicaIds){
                futures.add(this.node.delete(replicaId));
            }

            for(int i = 0; i < futures.size(); i++){
                CompletableFuture<NodeInfo> future = futures.get(i);

                try{
                    NodeInfo result = future.get();
                    retMsg.append("Successfully deleted replica " + i + " with hash " + replicaIds[i].getHash() + " that was in node " + result.id + "\n");
                } catch (InterruptedException | ExecutionException e) {
                    retMsg.append("Could not delete replica " + i + " with hash " + replicaIds[i].getHash() + "\n");
                }
            }

            this.node.getBackupManager().getDesiredFileRepDegrees().computeIfPresent(fileId, (k, _v) -> this.node.getBackupManager().getDesiredFileRepDegrees().remove(k));

        } catch (IOException | NoSuchAlgorithmException e) {
            return "Failed to generate replica ids";
        }


        return retMsg.toString();
    }

    public CompletableFuture<NodeInfo> deleteReplica(ReplicaIdentifier replicaId) {

        CompletableFuture<NodeInfo> result = new CompletableFuture<NodeInfo>();

        try {

            SSLServerSocket tempSocket = Network.createServerSocket(0);
            tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

            NodeInfo targetNode = this.node.findSuccessor(replicaId.getHash());

            CompletableFuture<ChordMessage> future = this.node.getCommunicator().async_listenOnSocket(tempSocket);

            DeleteReplicaMessage msg = new DeleteReplicaMessage(new SimpleNodeInfo(this.node.getNodeInfo().address, tempSocket.getLocalPort()), replicaId);

            this.node.getCommunicator().send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

            ChordMessage response = future.get();


            if(response instanceof NotFoundMessage){
                result.completeExceptionally(new Exception("Could not find file to delete!"));
                return result;
            }

            result.complete(new NodeInfo(((DeleteConfirmationMessage) response).getNode()));

        } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
            result.completeExceptionally(new Exception("Could not delete file. Maybe it is not in the network"));
        }

        return result;
    }

    public synchronized void deleteReplica(DeleteReplicaMessage msg) {
        try {
            if(this.node.getState().deleteReplica(msg.getReplicaId())){

                if(!this.node.getState().hasFileReplicas(msg.getReplicaId().getFileId())) {
                    FileManager.deleteFile(Paths.get(
                            Node.NODE_PATH,
                            "backup",
                            String.valueOf(msg.getReplicaId().getFileId().hashCode())).toString());
                }

                DeleteConfirmationMessage confirmation = new DeleteConfirmationMessage(new SimpleNodeInfo(this.node.getNodeInfo()));

                this.node.getState().removeReplicaLocation(msg.getReplicaId());

                this.node.getCommunicator().send(Utils.createClientSocket(msg.getNode().address, msg.getNode().port), confirmation);

            }else if(this.node.getState().hasReplicaLocation(msg.getReplicaId())){
                SimpleNodeInfo targetNode = this.node.getState().getReplicasLocation().get(msg.getReplicaId());

                DeleteReplicaMessage nextMsg = new DeleteReplicaMessage(msg.getNode(), msg.getReplicaId());

                this.node.getCommunicator().send(Utils.createClientSocket(targetNode.address, targetNode.port), nextMsg);

                this.node.getState().removeReplicaLocation(msg.getReplicaId());

            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
