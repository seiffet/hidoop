package hdfs;


import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.io.PrintWriter;
import java.util.HashMap;
import java.lang.NullPointerException;

public class ThreadDelete implements Runnable {

	private int bloc;
	private ArrayList<String> repartitionblocmachines;
	private String nomfichier;

	public ThreadDelete(int bloc, ArrayList<String> repartitionblocmachines, String nomfichier) {
		this.bloc = bloc;
		this.repartitionblocmachines = repartitionblocmachines;
		this.nomfichier = nomfichier;
	}

	public void run() {
		for (String machine : repartitionblocmachines) {
			try {
				String[] machine_split = machine.split("//");
				System.out.println(machine);
				Socket socket_datanode_client = new Socket(machine_split[0],Integer.parseInt(machine_split[1]));
				System.out.println("Working");
				PrintWriter pw = new PrintWriter(socket_datanode_client.getOutputStream(), true);
				pw.println("DL" + "//" + nomfichier + "//" + bloc);
				System.out.println("DL" + "//" + nomfichier + "//" + bloc);
				pw.close();
				socket_datanode_client.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	}