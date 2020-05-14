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
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.FileReader;

public class ThreadWriteHelper implements Runnable {

	private Socket clientso;
    private int port;

	public ThreadWriteHelper(Socket so, int port) {
		clientso=so;
        this.port=port;
	}


	public void run() {

            try {
            BufferedReader br1 = new BufferedReader(new InputStreamReader(clientso.getInputStream()));
			PrintWriter pw1 = new PrintWriter(clientso.getOutputStream(), true);
            
            while(true){ // n'est pas géniale 	
                try {
				String commande = (String) br1.readLine();
				String[] commande_info = commande.split("//");
				String[] c = commande_info[1].split(".txt");
				commande_info[1] = c[0];
				//System.out.println(commande_info[1]+"Commande");
				/* commande_info[1] = commande_info[1].split(".")[0]; */
				if (commande_info[0].equals("WR")) {
					while (true) {
					String donnees = (String) br1.readLine();
					if (donnees.equals("END")) {
						break;
					}
					String [] infofile = commande_info[1].split("/");
					String filename = infofile[infofile.length-1];
					commande_info[1]=filename;
					File directory_fichiersource = new File("/work/DataNode" + (port - 8001) + "/" + commande_info[1]);
					if (!directory_fichiersource.exists()) {
						directory_fichiersource.mkdir();
					}
					if (commande_info[2].equals("LINE")) {
						File bloc_courant = new File("/work/DataNode" + (port - 8001) + "/" + commande_info[1]+"/"+"BLOC"+commande_info[3]+".hdfs");
						if (!bloc_courant.exists()) {
							bloc_courant.createNewFile();
						}
						FileWriter fw = new FileWriter(bloc_courant,true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(donnees);
						bw.close();
						fw.close();
					} else {
						File bloc_courant = new File("/work/DataNode" + (port - 8001) + "/" + commande_info[1]+"/"+"BLOC"+commande_info[3]+".hdfs");
						if (!bloc_courant.exists()) {
							bloc_courant.createNewFile();
						}
						FileWriter fw = new FileWriter(bloc_courant,true);
						fw.write(donnees + "\n");
						fw.close();
					}
				}
				}	
					// pour test
					
					else if (commande_info[0].equals("RD")) {
					
					String [] infofile = commande_info[1].split("/");
					String filename = infofile[infofile.length-1];
					commande_info[1]=filename;

					
					
					long octets = new File("DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".hdfs").length();
					BufferedReader bloc_lire = new BufferedReader(new FileReader("DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".hdfs"));
					
					String line;
					pw1.println(octets);
					while ((line = bloc_lire.readLine()) != null) {
						pw1.println(line);
					}
					br1.close();
					pw1.close();
					bloc_lire.close();
					clientso.close();	
				} else 	if (commande_info[0].equals("RDM")) {
						
						String [] infofile = commande_info[1].split("/");
						String filename = infofile[infofile.length-1];
						commande_info[1]=filename;
						
						
						long octets = new File(Project.PATH+"/DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".map").length();
						System.out.println(octets);
						BufferedReader bloc_lire = new BufferedReader(new FileReader(Project.PATH+"/DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[3] + ".map"));
						

						
						String line;
						pw1.println(octets);
						while ((line = bloc_lire.readLine()) != null) {
							pw1.println(line);
						}
						br1.close();
						pw1.close();
						bloc_lire.close();
						clientso.close();	
                        break;
					}
					
					else {
					File bloc_to_be_deleted = new File("DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[2] + ".hdfs");
					//System.out.println("DataNode" + (port - 8001) + "/" + commande_info[1] + "/" + "BLOC" + commande_info[2] + ".hdfs");
					boolean bool = bloc_to_be_deleted.delete();
					if (bool) {
						System.out.println("DONE");
						}
					}
				
                } catch (NullPointerException e) {
                    break;
                }
		}
            } catch (Exception e){
                e.printStackTrace();
            }     
    }






}















