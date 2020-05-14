
package hdfs;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import formats.Format;
import formats.KV;

import java.io.PrintWriter;
import java.util.HashMap;
import java.lang.NullPointerException;
import java.lang.StringBuffer;

public class ThreadWrite implements Runnable {

	private Format fichiersource;
	private String fichiersourcetype;
	private HashMap<Integer,ArrayList<String>> listeblocsmachines;
	private long facteur_fragmentation;
	private int nombre_blocs;

	public ThreadWrite(Format fichiersource, String fichiersourcetype, HashMap<Integer,ArrayList<String>> listeblocsmachines, long facteur_fragmentation, int nombre_blocs) {
		this.fichiersource = fichiersource;
		this.fichiersourcetype = fichiersourcetype;
		this.listeblocsmachines = listeblocsmachines;
		this.facteur_fragmentation = facteur_fragmentation;
		this.nombre_blocs = nombre_blocs;
	}

	/* Information importante à savoir : chaque caractére est codé sur deux octets */
	/* On a besoin d'écrire chaque bloc dans la liste de machine où il doit être stocker 
	(Le DataNode s'occupera de stocker le bloc : notre objective c'est d'envoyer à chaque datanode le bloc à stocker) */
	public void run() {
		try {
		fichiersource.open(Format.OpenMode.R);
		for (int i : listeblocsmachines.keySet()) {
			ArrayList<String> machines = listeblocsmachines.get(i);
			long index_aux = 0L;
			HashMap<String,Socket> socket_datanode_client = new HashMap<String,Socket>();
			PrintWriter pw =null;
			StringBuffer buffer = new StringBuffer();
			String[] machine_split;
			for (String machine : machines) {	
				machine_split = machine.split("//");
				socket_datanode_client.put(machine,new Socket(machine_split[0],Integer.parseInt(machine_split[1])));
				pw = new PrintWriter(socket_datanode_client.get(machine).getOutputStream(), true);
				pw.println("WR" + "//" + fichiersource.getFname() + "//" + "LINE" + "//" + Integer.toString(i));
				
			}
			
			while (index_aux < facteur_fragmentation) {
				if (fichiersourcetype.equals("LINE")) {
					try {
						KV ligne = fichiersource.read();
						index_aux = index_aux + ligne.v.length()*8;
						buffer.append(ligne.v);

					}catch (NullPointerException n) {
						fichiersource.close();
						return;
					}catch (Exception e) {
						e.printStackTrace();
					}
				}else {
					try {
						KV clevaleur = fichiersource.read();
						index_aux = index_aux + (clevaleur.v.length()+clevaleur.k.length()+KV.SEPARATOR.length())*8;
						for (String machine : machines) {
							machine_split = machine.split("//");
							Socket ssocket_datanode_client = new Socket(machine_split[0],Integer.parseInt(machine_split[1]));
							pw = new PrintWriter(ssocket_datanode_client.getOutputStream(), true);
							pw.println("WR" + "//" + fichiersource.getFname() + "//" + "KV" + "//" + Integer.toString(i));
							System.out.println("WR" + "//" + fichiersource.getFname() + "//" + "KV" + "//" + Integer.toString(i));
							pw.println(clevaleur.k + KV.SEPARATOR + clevaleur.v);
							pw.close();
							ssocket_datanode_client.close();
						}
					}catch (NullPointerException n) {
						fichiersource.close();
						return;
					}catch (Exception e) {
							e.printStackTrace();
					}
				}	
			}

		for (String machine : machines) {
				pw.println(buffer);	
				pw.println("END");
				pw.flush();
				socket_datanode_client.get(machine).close();		
			}	
		}
		}catch (Exception e) {
		e.printStackTrace();
	}
	}	
}	
