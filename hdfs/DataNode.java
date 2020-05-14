package hdfs;

import java.net.InetAddress;
import java.net.Socket;
import java.rmi.RemoteException;

import config.Project;
import ordo.DaemonImpl;

import java.net.ServerSocket;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.FileReader;

public class DataNode {
	public int port;
	public String nommachine;

	public DataNode(int port) {
		this.port = port;
		this.nommachine = "DataNode" + (this.port - 8001);
	}
	public static void main (String[] args) {

		/* L'enregistrement auprés du NameNode */

		/* 1 ére étape on a besoin de l'adresse IP du DataNode c'est à dire de la machine sur laquelle il est lancé
		   puis l'adresse IP du NameNode auquels on va s'enregistrer */
		try {
		InetAddress adresse = InetAddress.getLocalHost();
		InetAddress adressenamenode = InetAddress.getByName(NameNode.adresse_namenode);

		/* On se connecte maitenant au NameNode et on envoie l'adresse du DataNode puis on reçoit le port à utiliser pour la communication */
		Project p = new Project();
		
		Socket s = new Socket(p.getAdresseNameNode(), p.getNameNodeDataPort());
		System.out.println("---------------------------------------");
		System.out.println("The DataNode is launched.");
		System.out.println("---------------------------------------");
		BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
		PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
		/* On envoie l'adresse IP du DataNode */
		pw.println(adresse.getHostAddress());
		String portstr = (String) br.readLine();
		int port = Integer.parseInt(portstr);
		System.out.println("---------------------------------------");
		System.out.println("I'm connected to the NameNode. My communication port with the client is : " + port);
		System.out.println("---------------------------------------");
		br.close();
		pw.close();
		s.close();
		
		/* On est prêt maitenant à lancer notre DataNode avec le bon port de communication pour ensuite recevoir les différents rêquéte du client */
	
		new DataNode(port);
		
		File directory_datanode = new File("/work/DataNode" + (port - 8001));
		directory_datanode.mkdir();
		
		// pour tes avec mapredouce 
		Thread thd=new Thread( new Runnable() {
			public void run() {
				try {
					(new DaemonImpl(port - 8001)).start_daemon(port+1000);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		thd.start();
		
		
		
		// pour test avec mapreduce
		
		
		/* Traitement des rêquéte du client */
		/* Format de la commande : commande[0] : RD | WR | DL ; commande[1] : nomfichier ; commande[2] : facteur_duplication ; commande[3] : nombre de blocs */
		
		/* On accepte la connexion du client */
			
			ServerSocket ss = new ServerSocket(port);
			
			int i=0;
			while (true) {
				Socket so = ss.accept();


				Thread thclient = new Thread(new ThreadWriteHelper(so,port));
				thclient.start();
				
		}








		
		/*ThreadWriter th = new ThreadWriter(ss,nm);
		Thread t = new Thread(th);
		/* On lance le thread responsable à écrire dans le bloc coresspondant suivant les informations reçus par le client 
		t.start();*/
	}catch (Exception e) {
		e.printStackTrace();
	}
	}
}
