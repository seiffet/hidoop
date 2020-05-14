package config;

import java.net.InetAddress;
import java.util.ArrayList;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.NameNode;

public class Project {
	private static int nameNodeClientPort;
	private static int nameNodeDataPort;
	private static int deamonmasterPort;
	private String adresse_namenode;
	public static String PATH="/work";
	public static int DaemonNum = 3 ;
	public static ArrayList<String> adresslistDaemon= new ArrayList<>();
	public static ArrayList<Integer> portlistDaemon= new ArrayList<>();
	public static int DaemonMasterport= 9999 ;
	public static String Daemonmaseterrmiadress="//localhost:" + DaemonMasterport + "/DaemonMaster";
	public static int reduceMethod = 0;
	public static int facteurdedubplication = 1;
	
	public static int facteurfragmentation = 64;
	public static ArrayList<String> getAdresslistDaemon() {
		return adresslistDaemon;
	}
	
	public static void setAdresslistDaemon(ArrayList<String> listDaemon) {
		adresslistDaemon=listDaemon;
	}
	
	

	public static ArrayList<Integer> getPortlistDaemon() {
		return portlistDaemon;
	}



	public Project() {
		// peut etre automatisé !!
		int port = 10000 ;
		
		for (int i = 0 ; i< DaemonNum ; i++) {
			portlistDaemon.add(port);
			String ip = "//localhost:"+(port++)+"/Daemon" +i;
			adresslistDaemon.add(ip);
		}
		
		int compteur = 0;
		try {
		InetAddress adresse = InetAddress.getLocalHost();
		KVFormat infos_builder = new KVFormat("./config/builder.txt");
		infos_builder.open(Format.OpenMode.R);
		while (compteur < 4) {
			KV clevaleur = infos_builder.read();
			if (clevaleur.k.equals("nameNodeClientPort")) {
				nameNodeClientPort = Integer.parseInt(clevaleur.v);
				compteur++;
			} else if (clevaleur.k.equals("nameNodeDataPort")) {
				nameNodeDataPort = Integer.parseInt(clevaleur.v);
				compteur++;
			} else if (clevaleur.k.equals("adresse_namenode")) {
				adresse_namenode = clevaleur.v;
				compteur++;
			} else if (clevaleur.k.equals("deamonmasterPort")) {
				deamonmasterPort = Integer.parseInt(clevaleur.v);
				compteur++;
			}
		}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}



	public static void selectreduceMethod(int i) {
		// TODO Auto-generated method stub
		reduceMethod = i;
	}

	public void buildNameNode(NameNode n) {
		
		n.setnameNodeClientPort(nameNodeClientPort);
		n.setnameNodeDataPort(nameNodeDataPort);
		n.setadresseNameNode(adresse_namenode);
		n.setdeamonmasterPort(deamonmasterPort);
	}

	public static void selectnumberofDaomon(int i) {
		// TODO Auto-generated method stub
		DaemonNum = i ;
	}

	public static void setPortlistDaemon(ArrayList<Integer> listport) {
		// TODO Auto-generated method stub
		
		portlistDaemon = listport ;
		
	}

	public String getAdresseNameNode() {
		// TODO Auto-generated method stub
		return adresse_namenode;
	}

	public int getNameNodeDataPort() {
		// TODO Auto-generated method stub
		return nameNodeDataPort;
	}
}
