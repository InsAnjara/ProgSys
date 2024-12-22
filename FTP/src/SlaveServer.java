import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlaveServer {

    private static final int BROADCAST_PORT = 1234; // Port pour les broadcasts
    private static final int RESPONSE_PORT = 1235; // Port pour écouter les commandes
    private static final String STORAGE_DIRECTORY = "slave_storage"; // Répertoire pour stocker les fichiers
    private Map<String, String> fileMap = new HashMap<>(); // Map pour associer les fichiers aux chemins locaux
    private volatile boolean running = true;

    public static void main(String[] args) {
        SlaveServer slaveServer = new SlaveServer();
        try {
            slaveServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        System.out.println("SlaveServer démarré. En attente des requêtes...");

        // Assurez-vous que le répertoire de stockage existe
        File storageDir = new File(STORAGE_DIRECTORY);
        if (!storageDir.exists() && !storageDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire de stock");
        }

        // Écoute en parallèle pour les broadcasts et les requêtes directes
        Thread broadcastListener = new Thread(this::listenForBroadcasts);
        Thread requestListener = new Thread(this::listenForRequests);

        broadcastListener.start();
        requestListener.start();

        try {
            broadcastListener.join();
            requestListener.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void listenForBroadcasts() {
        try (DatagramSocket socket = new DatagramSocket(BROADCAST_PORT)) {
            socket.setBroadcast(true);
            byte[] buffer = new byte[1024];

            System.out.println("En attente de messages de broadcast...");

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength());
                InetAddress masterAddress = packet.getAddress();
                int masterPort = RESPONSE_PORT; // Le port où le Master écoute les réponses

                System.out.println("Message de broadcast reçu : " + message + " de " + masterAddress.getHostAddress());

                if ("DISCOVER_SLAVES".equalsIgnoreCase(message.trim())) {
                    respondToMaster(masterAddress, masterPort);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void respondToMaster(InetAddress masterAddress, int masterPort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            String response = "SLAVE_AVAILABLE";
            byte[] buffer = response.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, masterAddress, masterPort);

            socket.send(packet);
            System.out.println("Réponse envoyée au MasterServer : " + masterAddress.getHostAddress() + ":" + masterPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenForRequests() {
        try (ServerSocket serverSocket = new ServerSocket(RESPONSE_PORT)) {
            System.out.println("En attente des connexions sur le port " + RESPONSE_PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Connexion reçue de : " + clientSocket.getInetAddress().getHostAddress());

                // Traiter la requête dans un thread séparé
                new Thread(() -> handlePersistentConnection(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handlePersistentConnection(Socket socket) {
        try (DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            String clientAddress = socket.getInetAddress().getHostAddress();
            System.out.println("Établissement d'une connexion persistante avec : " + clientAddress);

            while (running && !socket.isClosed()) {
                try {
                    String command = dis.readUTF();
                    System.out.println("Commande reçue de " + clientAddress + " : " + command);

                    if ("QUIT".equals(command)) {
                        System.out.println("Déconnexion demandée par " + clientAddress);
                        break;
                    }

                    switch (command) {
                        case "ADD_PART":
                            receivePart(dos, dis);
                            break;
                        case "GET_PART":
                            sendPart(dos, dis);
                            break;
                        case "REMOVE_PART":
                            removePart(dos, dis);
                            break;
                        case "CHECK":
                            checkFile(dis, dos);
                            break;
                        default:
                            System.out.println("Commande inconnue reçue de " + clientAddress + " : " + command);
                    }

                    dos.flush();
                } catch (EOFException e) {
                    System.out.println("Client " + clientAddress + " déconnecté");
                    break;
                } catch (SocketException e) {
                    System.out.println("Connexion interrompue avec " + clientAddress);
                    break;
                } catch (IOException e) {
                    System.err.println("Erreur avec le client " + clientAddress + ": " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur fatale dans la connexion persistante: " + e.getMessage());
        } finally {
            try {
                if (!socket.isClosed()) {
                    socket.close();
                }
                System.out.println("Connexion fermée avec : " + socket.getInetAddress().getHostAddress());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private void checkFile(DataInputStream dis, DataOutputStream dos) throws IOException {
        String fileName = dis.readUTF();
        File fil = new File(STORAGE_DIRECTORY);
        List<File> files = List.of(fil.listFiles());
        int count = 0;
        for (File file : files) {
            if (file.exists() && file.getName().startsWith(fileName)) {
                count++;
            }
        }
        dos.writeInt(count);
    }

    private void receivePart(DataOutputStream dos, DataInputStream dis) throws IOException {
        try {
            System.out.println("Réception d'une partie de fichier...");
            File file = FileTransferUtils.receiveFile(dis, STORAGE_DIRECTORY);
            fileMap.put(file.getName(), file.getAbsolutePath());

            // Envoyer confirmation au master
            dos.writeUTF("SUCCESS");
            dos.flush();

            System.out.println("Partie de fichier reçue avec succès !");
        } catch (IOException e) {
            dos.writeUTF("ERROR");
            dos.flush();
            throw e;
        }
    }

    private void sendPart(DataOutputStream dos, DataInputStream dis) throws IOException {
        String partName = dis.readUTF();
        File partFile = new File(STORAGE_DIRECTORY, partName);

        if (partFile.exists()) {
            FileTransferUtils.sendFile(dos, partFile.getAbsolutePath());
            dos.writeUTF("SUCCESS");
        } else {
            dos.writeLong(-1); // Indiquer que le fichier n'existe pas
            System.out.println("Erreur : Partition manquante - " + partName);
        }
    }

    private void removePart(DataOutputStream dos, DataInputStream dis) throws IOException {
        String fileName = dis.readUTF();
        System.out.println("Suppression de la partie : " + fileName);

        boolean success = false;
        File storageDir = new File(STORAGE_DIRECTORY);
        File[] files = storageDir.listFiles((dir, name) -> name.startsWith(fileName + ".part"));

        if (files != null) {
            success = true;
            for (File file : files) {
                if (!file.delete()) {
                    success = false;
                    System.err.println("Échec de la suppression du fichier : " + file.getName());
                }
            }
        }

        dos.writeUTF(success ? "SUCCESS" : "ERROR");
        if (success) {
            fileMap.remove(fileName);
            System.out.println("Partie(s) supprimée(s) avec succès !");
        }
    }

}
