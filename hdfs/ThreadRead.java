package hdfs;


import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.io.PrintWriter;
import java.util.HashMap;
import java.lang.NullPointerException;
import java.util.Random;

import formats.Format;
import formats.KV;

import java.io.InputStreamReader;
import java.io.BufferedReader;

public class ThreadRead implements Runnable {

	private Format fichierdestination;
	private String formatfichiertype;
	private String nomfichier;
	private HashMap<Integer,ArrayList<String>> repartitionfichier;

	
	public ThreadRead(Format fichierdestination, String nomfichier, String formatfichiertype, HashMap<Integer,ArrayList<String>> repartitionfichier) {
		this.fichierdestination = fichierdestination;
		this.nomfichier = nomfichier;
		this.formatfichiertype = formatfichiertype;
		this.repartitionfichier = repartitionfichier;
	}

	public void run() {
		try {
		fichierdestination.open(Format.OpenMode.W);
		for (int bloc : repartitionfichier.keySet()) {
			Long taille = 0L;
			ArrayList<String> duplication_bloc = new ArrayList<String> (repartitionfichier.get(bloc));
			Random rand = new Random();
			int r = rand.nextInt(duplication_bloc.size());
			String machine = duplication_bloc.get(r);
			String[] machine_split = machine.split("//");
			Socket socket_datanode_client = new Socket(machine_split[0],Integer.parseInt(machine_split[1]));
			PrintWriter pw = new PrintWriter(socket_datanode_client.getOutputStream(), true);
			BufferedReader br = new BufferedReader(new InputStreamReader(socket_datanode_client.getInputStream()));
			pw.println("RD" + "//" + nomfichier + "//" + formatfichiertype + "//" + Integer.toString(bloc));
			String taillebloc = (String) br.readLine();
			while (taille < Long.parseLong(taillebloc)) {
				String donnees = (String) br.readLine();
				
				System.out.println("les donn lu"+donnees);
				try {
				if (formatfichiertype.equals("LINE")) {
					KV clevaleur = new KV(Integer.toString(donnees.length()),donnees);
					fichierdestination.write(clevaleur);
				}else{
					String[] donnees_split = donnees.split(KV.SEPARATOR);
					KV clevaleur = new KV(donnees_split[0],donnees_split[1]);
					fichierdestination.write(clevaleur);
				}
				} catch (NullPointerException n) {
					pw.close();
					br.close();
					socket_datanode_client.close();
					break;
				}
				
				taille = taille + donnees.length();
			}
			pw.close();
			br.close();
			socket_datanode_client.close();
		}
		fichierdestination.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}

