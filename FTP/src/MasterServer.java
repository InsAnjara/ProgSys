import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class MasterServer {

    static final int CLIENT_PORT = 1234; // Port pour le client
    static final int BROADCAST_PORT = 1234; // Port pour les broadcasts
    static final int RESPONSE_PORT = 1235; // Port pour les réponses des slaves
    private Map<String, List<SlaveInfo>> fileLocations = new ConcurrentHashMap<>(); // Nom du fichier -> Liste des adresses des slaves contenant les parties
    private List<SlaveInfo> activeSlaves = new ArrayList<>(); // Liste des slaves actifs

    public Map<String, List<SlaveInfo>> getFileLocations() {
        return fileLocations;
    }

    public List<SlaveInfo> getActiveSlaves() {
        return activeSlaves;
    }

    public static void main(String[] args) {
        MasterServer masterServer = new MasterServer();
        try {
            masterServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {
        System.out.println("MasterServer démarré sur le port " + CLIENT_PORT);

        try (ServerSocket serverSocket = new ServerSocket(CLIENT_PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connecté : " + clientSocket.getInetAddress().getHostAddress());

                // Traiter le client dans un thread séparé
                new Thread(new ClientHandler(this, clientSocket)).start();
            }
        }
    }

    public void discoverSlaves() {
        activeSlaves.clear();

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setBroadcast(true);

            String message = "DISCOVER_SLAVES";
            byte[] buffer = message.getBytes();

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), BROADCAST_PORT);
            socket.send(packet);

            System.out.println("Message de broadcast envoyé pour découvrir les slaves.");

            // Attendre les réponses des slaves
            listenForSlaveResponses();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenForSlaveResponses() {
        try (DatagramSocket socket = new DatagramSocket(RESPONSE_PORT)) {
            socket.setSoTimeout(5000); // Temps d'attente pour les réponses
    
            byte[] buffer = new byte[1024];
            System.out.println("En attente des réponses des slaves...");
    
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
    
                    String message = new String(packet.getData(), 0, packet.getLength());
                    if ("SLAVE_AVAILABLE".equalsIgnoreCase(message.trim())) {
                        InetSocketAddress slaveAddress = new InetSocketAddress(packet.getAddress(), packet.getPort());
                        activeSlaves.add(new SlaveInfo(slaveAddress.getAddress().getHostAddress(), slaveAddress.getPort()));
    
                        System.out.println("Slave détecté : " + slaveAddress.getAddress().getHostAddress());
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("Fin de la période d'attente des réponses.");
                    break;
                }
            }
    
            System.out.println("Slaves actifs détectés : " + activeSlaves);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getFilePartitionCount(String filename) {
        return fileLocations.get(filename) == null ? 0 : fileLocations.get(filename).size();
    }
}



