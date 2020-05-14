package hdfs;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

public class EnregistreurDataNode implements Runnable {

	private ServerSocket s;
	private int id_datanode;

	public EnregistreurDataNode(ServerSocket s) {
		this.s = s;
		this.id_datanode = 0;
	}

	public void run() {
		try {

			while (true) {
			/* Chaque DataNode aura un identifiant unique et par la suite un port de communication unique */
			id_datanode = id_datanode + 1;
			Socket ss = this.s.accept();
			/* System.out.println("Le DataNode "+i+" vient de s'enregistrer au NameNode. Leurs port de communication est " + (10001 + i)); */
			/* Plusieurs datanodes chercherons à s'enregistrer à la fois chacun d'eux lancera un thread indépendant de l'autre qui leurs permettera de s'enregistrer */
			System.out.println("The connection of the DataNode " + id_datanode + " will start.");
			EnregistreurHelper enregistrementserveurtest = new EnregistreurHelper(ss,id_datanode);
			Thread t = new Thread(enregistrementserveurtest);
			t.start();
			
		

			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}