import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;

public class SlaveServer {
    private static int BROADCAST_PORT;
    private String STORAGE_DIRECTORY;
    private Map<String, String> fileMap = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private int commandPort;
    private static final String CONFIG_FILE = "configSlave.properties";

    public static void main(String[] args) {
        SlaveServer slaveServer = new SlaveServer();
        try {
            Properties config = loadConfiguration();
            int commandPort = Integer.parseInt(config.getProperty("commandPort", "1235"));
            slaveServer.start(commandPort, config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Properties loadConfiguration() throws IOException {
        Properties props = new Properties();
        File configFile = new File(CONFIG_FILE);

        if (configFile.exists()) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                props.load(fis);
                System.out.println("Configuration chargée depuis " + CONFIG_FILE);
            }
        } else {
            System.out.println("Fichier de configuration non trouvé, utilisation des valeurs par défaut");
            props.setProperty("broadcastPort", "1234");
            props.setProperty("commandPort", "1236");
            props.setProperty("storageDirectory", "slave_storage");

            // Création du fichier de configuration avec les valeurs par défaut
            try (FileOutputStream fos = new FileOutputStream(configFile)) {
                props.store(fos, "Configuration du SlaveServer");
                System.out.println("Fichier de configuration créé avec les valeurs par défaut");
            }
        }

        return props;
    }

    public void start(int commandPort, Properties config) throws IOException {
        this.commandPort = commandPort;
        BROADCAST_PORT = Integer.parseInt(config.getProperty("broadcastPort", "1234"));
        STORAGE_DIRECTORY = config.getProperty("storageDirectory", "slave_storage");

        System.out.println("SlaveServer démarré sur le port " + commandPort);
        System.out.println("Port de broadcast: " + BROADCAST_PORT);
        System.out.println("Répertoire de stockage: " + STORAGE_DIRECTORY);

        File storageDir = new File(STORAGE_DIRECTORY);
        if (!storageDir.exists() && !storageDir.mkdirs()) {
            throw new IOException("Impossible de créer le répertoire de stockage");
        }

        Thread broadcastListener = new Thread(this::listenForBroadcasts);
        Thread requestListener = new Thread(() -> listenForRequests(commandPort));

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
        try (DatagramSocket socket = new DatagramSocket(null)) { // Socket non liée
            socket.setReuseAddress(true); // <-- Configuration AVANT le bind
            socket.bind(new InetSocketAddress(BROADCAST_PORT)); // Lie le port APRÈS setReuseAddress
            socket.setBroadcast(true);

            byte[] buffer = new byte[1024];
            System.out.println("En attente de messages de broadcast sur le port " + BROADCAST_PORT + "...");

            while (running) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    InetAddress masterAddress = packet.getAddress();

                    if ("DISCOVER_SLAVES".equalsIgnoreCase(message.trim())) {
                        respondToMaster(masterAddress, commandPort);
                    }
                } catch (IOException e) {
                    if (!running) break;
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void respondToMaster(InetAddress masterAddress, int slavePort) {
        try (DatagramSocket socket = new DatagramSocket()) {
            String response = "SLAVE_AVAILABLE:" + slavePort;
            byte[] buffer = response.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, masterAddress, 1235);
            socket.send(packet);
            System.out.println("Réponse envoyée au Master avec le port: " + slavePort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenForRequests(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("En attente des connexions sur le port " + port);
            while (running) {
                Socket clientSocket = serverSocket.accept();
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

        synchronized (partName.intern()) {
            if (partFile.exists()) {
                FileTransferUtils.sendFile(dos, partFile.getAbsolutePath());
                dos.writeUTF("SUCCESS");
            } else {
                dos.writeLong(-1);
                System.out.println("Erreur : Partition manquante - " + partName);
            }
        }
    }

    private void removePart(DataOutputStream dos, DataInputStream dis) throws IOException {
        String fileName = dis.readUTF();
        System.out.println("Suppression de la partie : " + fileName);

        synchronized (fileName.intern()) {
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
}