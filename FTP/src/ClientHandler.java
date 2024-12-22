import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ClientHandler implements Runnable {
    private final MasterServer masterServer;
    private Socket clientSocket;

    public ClientHandler(MasterServer masterServer, Socket clientSocket) {
        this.masterServer = masterServer;
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
             DataInputStream dis = new DataInputStream(clientSocket.getInputStream())) {

            while (true) {
                String command = dis.readUTF();
                if ("QUIT".equals(command)) {
                    System.out.println("Client déconnecté.");
                    break;
                }

                System.out.println("Commande reçue : " + command);

                switch (command) {
                    case "LIST":
                        handleList(dos, dis);
                        break;
                    case "ADD":
                        handleAdd(dos, dis);
                        break;
                    case "GET":
                        handleGet(dos, dis);
                        break;
                    case "REMOVE":
                        handleRemove(dos, dis);
                        break;
                    default:
                        System.out.println("Commande inconnue : " + command);
                }
                dos.flush();
            }
        } catch (IOException e) {
            System.err.println("Erreur de communication avec le client : " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("Erreur lors de la fermeture de la connexion : " + e.getMessage());
            }
        }
    }

    // Modifier la méthode handleList :
    private void handleList(DataOutputStream dos, DataInputStream dis) throws IOException {
        System.out.println("Envoi de la liste des fichiers...");
        Map<String, List<SlaveInfo>> files = masterServer.getFileLocations();

        // Envoyer le nombre total de fichiers
        dos.writeInt(files.size());
        dos.flush();

        // Pour chaque fichier, envoyer son nom et le nombre de parties
        for (Map.Entry<String, List<SlaveInfo>> entry : files.entrySet()) {
            dos.writeUTF(entry.getKey()); // Nom du fichier
            dos.writeInt(entry.getValue().size()); // Nombre de parties
            dos.flush();
        }
    }

    private void handleRemove(DataOutputStream dos, DataInputStream dis) throws IOException {
        String fileName = dis.readUTF();
        List<SlaveInfo> slaves = masterServer.getFileLocations().get(fileName);

        if (slaves == null || slaves.isEmpty()) {
            dos.writeUTF("Erreur: Fichier non trouvé");
            return;
        }

        boolean success = true;
        for (SlaveInfo slave : slaves) {
            success &= removeFromSlave(slave, fileName);
        }

        if (success) {
            masterServer.getFileLocations().remove(fileName);
            dos.writeUTF("Fichier supprimé avec succès");
        } else {
            dos.writeUTF("Erreur: La suppression a échoué sur certains slaves");
        }
    }

    private boolean removeFromSlave(SlaveInfo slave, String fileName) {
        try (Socket slaveSocket = new Socket(slave.getIp(), MasterServer.RESPONSE_PORT);
             DataOutputStream dos = new DataOutputStream(slaveSocket.getOutputStream());
             DataInputStream dis = new DataInputStream(slaveSocket.getInputStream())) {

            dos.writeUTF("REMOVE_PART");
            dos.writeUTF(fileName);

            String response = dis.readUTF();
            return "SUCCESS".equals(response);
        } catch (IOException e) {
            System.err.println("Erreur lors de la suppression sur le slave " + slave.getIp() + ": " + e.getMessage());
            return false;
        }
    }

    private boolean sendToSlave(SlaveInfo slaveAddress, File filePart) throws IOException {
        try (Socket slaveSocket = new Socket(slaveAddress.getIp(), MasterServer.RESPONSE_PORT);
            DataOutputStream dos = new DataOutputStream(slaveSocket.getOutputStream());
            DataInputStream dis = new DataInputStream(slaveSocket.getInputStream())) {

            dos.writeUTF("ADD_PART");
            dos.flush();

            FileTransferUtils.sendFile(dos, filePart.getAbsolutePath());

            // Attendre la confirmation du slave
            if (!slaveSocket.isClosed()) {
                String response = dis.readUTF();

                if (!"SUCCESS".equals(response)) {
                    throw new IOException("Le slave n'a pas pu recevoir le fichier correctement");
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    private void handleAdd(DataOutputStream dos, DataInputStream dis) throws IOException {
        masterServer.discoverSlaves();

        File tempDir = new File("/temp");
        if (!tempDir.exists() &&!tempDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire temporaire");
        }
        File file = FileTransferUtils.receiveFile(dis, tempDir.getAbsolutePath());

        synchronized (masterServer.getActiveSlaves()) {
            if (masterServer.getActiveSlaves().isEmpty()) {
                dos.writeUTF("ERROR: Aucun slave disponible pour stocker le fichier.");
                return;
            }

            File[] parts = FileTransferUtils.splitFile(file.getAbsolutePath(), masterServer.getActiveSlaves().size());
            List<SlaveInfo> slaveList = new ArrayList<>();
            boolean success = true;

            try {
                for (int i = 0; i < parts.length; i++) {
                    SlaveInfo slaveAddress = masterServer.getActiveSlaves().get(i);
                    try {
                        success &= sendToSlave(slaveAddress, parts[i]);
                        slaveList.add(slaveAddress);
                    } catch (IOException e) {
                        success = false;
                        System.err.println("Erreur lors de l'envoi à " + slaveAddress + ": " + e.getMessage());
                    }
                }

                if (success) {
                    masterServer.getFileLocations().put(file.getName(), slaveList);
                    dos.writeUTF("SUCCESS: Fichier stocké avec succès");
                } else {
                    dos.writeUTF("ERROR: Certaines parties du fichier n'ont pas pu être stockées");
                }
            } finally {
                // Nettoyer les fichiers temporaires
                FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
                for (File part : parts) {
                    FileTransferUtils.deleteDirectory(part.getAbsolutePath());
                }
            }
        }
    }

    private void handleGet(DataOutputStream dos, DataInputStream dis) throws IOException {
        masterServer.discoverSlaves();

        // Créer le répertoire temporaire avec un chemin absolu
        File tempDir = new File(System.getProperty("user.dir"), "temp");
        if (!tempDir.exists() && !tempDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire temporaire");
        }

        String fileName = dis.readUTF();
        System.out.println("Demande de téléchargement pour le fichier: " + fileName);

        int numPartitions = getFilePartitionCount(fileName);
        System.out.println("Nombre de partitions trouvées: " + numPartitions);

        if (numPartitions <= 0) {
            dos.writeUTF("Erreur : Informations sur le fichier introuvables.");
            return;
        }

        boolean allPartitionsFetched = fetchFromSlave(fileName, numPartitions, tempDir);

        if (!allPartitionsFetched) {
            dos.writeUTF("Erreur : Impossible de récupérer toutes les partitions.");
            FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
            return;
        }

        // Fusion et envoi du fichier
        try {
            sendMergedFile(dos, fileName, tempDir);
        } finally {
            // Nettoyage des fichiers temporaires
            FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
        }
    }

    private void sendMergedFile(DataOutputStream dos, String fileName, File tempDir) throws IOException {
        List<File> partFiles = new ArrayList<>();
        System.out.println("Recherche des parties dans: " + tempDir.getAbsolutePath());

        // Lister tous les fichiers dans le répertoire temporaire
        File[] files = tempDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith(fileName + ".part")) {
                    partFiles.add(file);
                    System.out.println("Partie trouvée: " + file.getName());
                }
            }
        }

        if (partFiles.isEmpty()) {
            dos.writeUTF("Erreur : Partitions introuvables pour " + fileName);
            System.out.println("Erreur : Aucune partition disponible pour " + fileName);
            return;
        }

        // Trier les parties par leur numéro
        partFiles.sort((f1, f2) -> {
            int num1 = extractPartNumber(f1.getName());
            int num2 = extractPartNumber(f2.getName());
            return Integer.compare(num1, num2);
        });

        // Créer le fichier fusionné dans le répertoire temporaire
        File mergedFile = new File(tempDir, fileName);
        if (mergeFiles(partFiles, mergedFile)) {
            System.out.println("Fichier fusionné créé: " + mergedFile.getAbsolutePath());
            dos.writeUTF("Fichier prêt pour téléchargement.");
            FileTransferUtils.sendFile(dos, mergedFile.getAbsolutePath());
            System.out.println("Fichier envoyé avec succès: " + fileName);
        } else {
            dos.writeUTF("Erreur : Échec de la fusion des parties du fichier.");
        }
    }

    private int extractPartNumber(String filename) {
        try {
            return Integer.parseInt(filename.substring(filename.lastIndexOf("part") + 4));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            return 0;
        }
    }

    private boolean mergeFiles(List<File> parts, File mergedFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(mergedFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            for (File part : parts) {
                if (!part.exists()) {
                    System.err.println("Partie manquante: " + part.getAbsolutePath());
                    return false;
                }

                try (FileInputStream fis = new FileInputStream(part);
                     BufferedInputStream bis = new BufferedInputStream(fis)) {

                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = bis.read(buffer)) != -1) {
                        bos.write(buffer, 0, bytesRead);
                    }
                }
            }
            return true;
        } catch (IOException e) {
            System.err.println("Erreur lors de la fusion des fichiers: " + e.getMessage());
            return false;
        }
    }

    private boolean fetchFromSlave(String fileName, int numPartitions, File tempDir) {
        masterServer.discoverSlaves();
        if (masterServer.getActiveSlaves().isEmpty()) {
            System.out.println("Aucun slave disponible");
            return false;
        }

        for (int i = 1; i <= numPartitions; i++) {
            String partName = fileName + ".part" + i;
            boolean partFetched = false;

            for (SlaveInfo slave : masterServer.getActiveSlaves()) {
                System.out.println("Tentative de téléchargement depuis " + slave.getIp() + ":" + slave.getPort());
                if (downloadPartFromServer(partName, slave.getIp(), slave.getPort(), tempDir)) {
                    System.out.println("Partition " + partName + " téléchargée avec succès");
                    partFetched = true;
                    break;
                }
            }

            if (!partFetched) {
                System.out.println("Impossible de récupérer la partition: " + partName);
                return false;
            }
        }

        return true;
    }

    private boolean downloadPartFromServer(String partName, String address, int port, File tempDir) {
        try (Socket slaveSocket = new Socket(address, MasterServer.RESPONSE_PORT);
             DataOutputStream dos = new DataOutputStream(slaveSocket.getOutputStream());
             DataInputStream dis = new DataInputStream(slaveSocket.getInputStream())) {

            dos.writeUTF("GET_PART");
            dos.writeUTF(partName);
            dos.flush();

            File receivedFile = FileTransferUtils.receiveFile(dis, tempDir.getAbsolutePath());
            String response = dis.readUTF();

            return "SUCCESS".equals(response) && receivedFile.exists();
        } catch (IOException e) {
            System.err.println("Erreur lors de la réception de la partition " + partName +
                    " sur " + address + ":" + port + ": " + e.getMessage());
            return false;
        }
    }

    private int getFilePartitionCount(String fileName) {
        int count = 0;
        for (SlaveInfo slave : masterServer.getActiveSlaves()) {
            try (
                    Socket socket = new Socket(slave.getIp(), MasterServer.RESPONSE_PORT);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream dis = new DataInputStream(socket.getInputStream())
            ) {
                dos.writeUTF("CHECK");
                dos.writeUTF(fileName);

                count += dis.readInt();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return count;
    }
}
