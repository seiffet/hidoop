package hdfs;

import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ThreadWriter implements Runnable {
	
	private ServerSocket s;
	private String nommachine;
	private boolean bool;

	public ThreadWriter(ServerSocket s, String nommachine) {
		this.s = s;
		this.nommachine = nommachine;
	}

	public void run() {
		try {
			while (true) {
				Socket so = s.accept();
				BufferedReader br1 = new BufferedReader(new InputStreamReader(so.getInputStream()));
				PrintWriter pw1 = new PrintWriter(so.getOutputStream(), true);
				String commande = (String) br1.readLine();
				String donnees = (String) br1.readLine();
				String[] commande_info = commande.split("//");
				String[] c = commande_info[1].split(".txt");
				commande_info[1] = c[0];
				/* commande_info[1] = commande_info[1].split(".")[0]; */
				System.out.println(commande_info[0]);
				if (commande_info[0].equals("WR")) {
					File directory_fichiersource = new File(this.nommachine + "/" + commande_info[1]);
					if (!directory_fichiersource.exists()) {
						directory_fichiersource.mkdir();
					}
					if (commande_info[2].equals("LINE")) {
						File bloc_courant = new File(this.nommachine + "/" + commande_info[1]+"/"+"BLOC"+commande_info[3]+".hdfs");
						if (!bloc_courant.exists()) {
							bloc_courant.createNewFile();
						}
						FileWriter fw = new FileWriter(bloc_courant,true);
						fw.write(donnees + "\n");
						fw.close();
						}
						br1.close();
						pw1.close();
						so.close();
						commande_info[0] = "END";		
				} else {
					System.out.println("commande rd");
					long octets = new File(this.nommachine + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".hdfs").length();
					BufferedReader bloc_lire = new BufferedReader(new FileReader(this.nommachine + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".hdfs"));
					String line;
					pw1.println(octets);
					while ((line = bloc_lire.readLine()) != null) {
						pw1.println(line);
					}
					br1.close();
					pw1.close();
					so.close();	
				}
				System.out.println(commande_info[0]);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}