import java.io.*;
import java.net.Socket;
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
        Map<String, List<List<SlaveInfo>>> files = masterServer.getFileLocations();

        // Envoyer le nombre total de fichiers
        dos.writeInt(files.size());
        dos.flush();

        // Pour chaque fichier, envoyer son nom et le nombre de parties
        for (Map.Entry<String, List<List<SlaveInfo>>> entry : files.entrySet()) {
            dos.writeUTF(entry.getKey()); // Nom du fichier
            dos.writeInt(entry.getValue().size()); // Nombre de parties
            dos.flush();
        }
    }

    private void handleRemove(DataOutputStream dos, DataInputStream dis) throws IOException {
        String fileName = dis.readUTF();
        List<List<SlaveInfo>> slaves = masterServer.getFileLocations().get(fileName);

        if (slaves == null || slaves.isEmpty()) {
            dos.writeUTF("Erreur: Fichier non trouvé");
            return;
        }

        boolean success = true;
        for (List<SlaveInfo> replicas : slaves) {
            for (SlaveInfo slave : replicas) {
                success &= removeFromSlave(slave, fileName);
            }
        }

        if (success) {
            masterServer.getFileLocations().remove(fileName);
            dos.writeUTF("Fichier supprimé avec succès");
        } else {
            dos.writeUTF("Erreur: La suppression a échoué sur certains slaves");
        }
    }

    private boolean removeFromSlave(SlaveInfo slave, String fileName) {
        try (Socket slaveSocket = new Socket(slave.getIp(), slave.getPort());
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
        System.out.println("Tentative d'envoi à " + slaveAddress.getIp() + ":" + slaveAddress.getPort());
        try (Socket slaveSocket = new Socket(slaveAddress.getIp(), slaveAddress.getPort());
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
        if (!tempDir.exists() && !tempDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire temporaire");
        }
        File file = FileTransferUtils.receiveFile(dis, tempDir.getAbsolutePath());

        // Créer une copie de la liste des slaves pour éviter les modifications concurrentes
        List<SlaveInfo> slaves;
        synchronized (masterServer.getActiveSlaves()) {
            slaves = new ArrayList<>(masterServer.getActiveSlaves());
        }

        if (slaves.isEmpty()) {
            dos.writeUTF("ERROR: Aucun slave disponible pour stocker le fichier.");
            return;
        }

        File[] parts = FileTransferUtils.splitFile(file.getAbsolutePath(), slaves.size());
        // Modifier la section d'envoi des parties :
        List<List<SlaveInfo>> slaveList = new ArrayList<>();
        boolean success = true;

        try {
            for (int i = 0; i < parts.length; i++) {
                List<SlaveInfo> replicas = new ArrayList<>();
                for (int j = 0; j < masterServer.getREPLICATION_FACTOR(); j++) {
                    int slaveIndex = (i + j) % slaves.size(); // Sélection circulaire
                    SlaveInfo slaveAddress = slaves.get(slaveIndex);
                    System.out.println("Envoi de la partie " + (i + 1) + " (réplique " + (j + 1) + ") à " + slaveAddress);
                    try {
                        if (sendToSlave(slaveAddress, parts[i])) {
                            replicas.add(slaveAddress);
                        }
                    } catch (IOException e) {
                        success = false;
                        System.err.println("Erreur lors de l'envoi à " + slaveAddress + ": " + e.getMessage());
                    }
                }
                slaveList.add(replicas);
            }

            if (success) {
                masterServer.getFileLocations().put(file.getName(), slaveList);
                dos.writeUTF("SUCCESS: Fichier stocké avec succès");
            } else {
                dos.writeUTF("WARNING: Certaines répliques n'ont pas pu être créées");
            }
        } finally {
            FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
            for (File part : parts) {
                FileTransferUtils.deleteDirectory(part.getAbsolutePath());
            }
        }
    }

    private void handleGet(DataOutputStream dos, DataInputStream dis) throws IOException {
        String fileName = dis.readUTF();
        System.out.println("Demande de téléchargement pour le fichier: " + fileName);

        // Récupérer les slaves associés à ce fichier
        List<List<SlaveInfo>> slavesPerPart  = masterServer.getFileLocations().get(fileName);
        if (slavesPerPart  == null || slavesPerPart .isEmpty()) {
            dos.writeUTF("Erreur : Fichier introuvable.");
            return;
        }

        // Créer un répertoire temporaire
        File tempDir = new File(System.getProperty("user.dir"), "temp_" + System.currentTimeMillis());
        if (!tempDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire temporaire");
        }

        boolean allPartsFetched = true;

        for (int i = 0; i < slavesPerPart.size(); i++) {
            String partName = fileName + ".part" + (i + 1);
            List<SlaveInfo> replicas = slavesPerPart.get(i);
            boolean partFetched = false;

            // Essayer chaque réplique jusqu'à réussite
            for (SlaveInfo slave : replicas) {
                if (downloadPartFromSlave(partName, slave, tempDir)) {
                    partFetched = true;
                    break;
                }
            }

            if (!partFetched) {
                allPartsFetched = false;
                break;
            }
        }


        if (!allPartsFetched) {
            dos.writeUTF("Erreur : Impossible de récupérer toutes les parties.");
            FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
            return;
        }

        try {
            sendMergedFile(dos, fileName, tempDir);
        } finally {
            FileTransferUtils.deleteDirectory(tempDir.getAbsolutePath());
        }
    }

    private boolean downloadPartFromSlave(String partName, SlaveInfo slave, File tempDir) {
        try (Socket slaveSocket = new Socket(slave.getIp(), slave.getPort());
             DataOutputStream dos = new DataOutputStream(slaveSocket.getOutputStream());
             DataInputStream dis = new DataInputStream(slaveSocket.getInputStream())) {

            dos.writeUTF("GET_PART");
            dos.writeUTF(partName);
            dos.flush();

            File receivedFile = FileTransferUtils.receiveFile(dis, tempDir.getAbsolutePath());
            String response = dis.readUTF();
            return "SUCCESS".equals(response) && receivedFile.exists();

        } catch (IOException e) {
            System.err.println("Échec du téléchargement de " + partName + " : " + e.getMessage());
            return false;
        }
    }

    private void sendMergedFile(DataOutputStream dos, String fileName, File tempDir) throws IOException {
        List<File> partFiles = new ArrayList<>();

        // 1. Récupérer toutes les parties et trier par numéro
        for (int i = 1; i <= getFilePartitionCount(fileName); i++) {
            File part = new File(tempDir, fileName + ".part" + i);
            if (part.exists()) {
                partFiles.add(part);
            }
        }

        // Trier explicitement par numéro de partie
        partFiles.sort(Comparator.comparingInt(f -> extractPartNumber(f.getName())));

        // 2. Vérifier que toutes les parties sont présentes
        if (partFiles.size() != getFilePartitionCount(fileName)) {
            dos.writeUTF("ERROR: Partitions manquantes (" + partFiles.size() + "/" + getFilePartitionCount(fileName) + ")");
            dos.flush();
            return;
        }

        File mergedFile = new File(tempDir, fileName);
        boolean mergeSuccess = mergeFiles(partFiles, mergedFile);

        if (mergeSuccess) {
            // 3. Vérifier le checksum du fichier fusionné
            String mergedChecksum = FileTransferUtils.calculateChecksum(mergedFile.getAbsolutePath());
            System.out.println("Checksum fusionné : " + mergedChecksum);

            // Envoyer les métadonnées
            dos.writeUTF("SUCCESS");
            dos.writeUTF(mergedFile.getName());
            dos.writeLong(mergedFile.length());
            dos.writeUTF(mergedChecksum);
            dos.flush();

            // 4. Envoyer le contenu du fichier
            try (FileInputStream fis = new FileInputStream(mergedFile);
                 BufferedInputStream bis = new BufferedInputStream(fis)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = bis.read(buffer)) != -1) {
                    dos.write(buffer, 0, bytesRead);
                }
                dos.flush();
                System.out.println("Fichier fusionné envoyé avec succès");
            }
        } else {
            dos.writeUTF("ERROR: Échec de la fusion.");
            dos.flush();
        }
    }

    // Méthode helper pour extraire le numéro de partie
    private int extractPartNumber(String filename) {
        try {
            int partIndex = filename.lastIndexOf(".part");
            if (partIndex == -1) return 0; // Format invalide
            String partStr = filename.substring(partIndex + 5); // ".part" a 5 caractères
            return Integer.parseInt(partStr);
        } catch (Exception e) {
            return 0;
        }
    }

    private boolean mergeFiles(List<File> parts, File mergedFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(mergedFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            for (File part : parts) {
                try (FileInputStream fis = new FileInputStream(part);
                     BufferedInputStream bis = new BufferedInputStream(fis)) {

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = bis.read(buffer)) != -1) {
                        bos.write(buffer, 0, bytesRead);
                    }
                }
            }
            bos.flush();
            return true;
        } catch (IOException e) {
            System.err.println("Erreur lors de la fusion : " + e.getMessage());
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
        try (Socket slaveSocket = new Socket(address, MasterServer.getResponsePort());
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
        List<List<SlaveInfo>> parts = masterServer.getFileLocations().get(fileName);
        return (parts != null) ? parts.size() : 0;
    }
}
