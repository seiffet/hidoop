package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class EnregistreurHelper implements Runnable {

	private Socket s;
	private int id_datanode;

	public EnregistreurHelper(Socket s, int id_datanode) {
		this.s = s;
		this.id_datanode = id_datanode;
	}

	public void run() {
		try {
			
		BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
		PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
		/* On récupére l'adresse IP du DataNode d'identifiant : id_datanode */
		String adresse = (String) br.readLine();
		int port = 8001 + id_datanode;
		System.out.println(port);
		/* On ajoute la machine avec son port de communication à la liste des machines fonctionnels dans le NameNode avec accés en exclusion mutuelle */
		synchronized(NameNode.machinesliste) {
			NameNode.machinesliste.put(port, adresse);
		}
		pw.println(Integer.toString(port));
		
		


		br.close();
		pw.close();
		s.close();

		}	catch (Exception e) {
		e.printStackTrace();
		}
	}
}