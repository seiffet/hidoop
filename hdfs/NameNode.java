package hdfs;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import config.Project;
import ordo.DaemonMasterImpl;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;



public class NameNode {

	
	public static int nameNodeClientPort;
	public static int nameNodeDataPort;
	public static  String adresse_namenode;
	public static int deamonmasterPort;
	
	/* Structure de donnée liant chaque fichier par son nom et la classe INoeud qui représente le fichier */

	public static HashMap<String, INoeud> catalogue;
	
	/* La liste des machines (datanodes). On associe son port de communication avec leurs adresse IP */

	public static Map<Integer, String> machinesliste;

	/* Les getteurs et setteurs avec un accés en exclusion mutuelle */

	public synchronized static Map<String, INoeud> getCatalogue() {
		return catalogue;
	}

	public synchronized static void setInfoFichiers(HashMap<String, INoeud> catalogue) {
		NameNode.catalogue = catalogue;
	}

	public static void main(String[] args) {

		/* On récupére les informations nécessaire au lancement du NameNode par le builder */
		Project p = new Project();
		
		p.buildNameNode(new NameNode());
		
		/* On Initialise le NameNode */
		//DaemonMasterImpl.startdemonmaster(deamonmasterPort);
		
		
		NameNode.catalogue = new HashMap<String, INoeud>();
		NameNode.machinesliste = new HashMap<Integer, String>();

		try {
		/* L'obtention de l'adresse IP de la machine où le NameNode est lancé */
		InetAddress adresse = InetAddress.getLocalHost();
		NameNode.adresse_namenode = adresse.getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		

		try {
			/* L'enregistrement des DataNodes souhaitant se connecter au NameNode */
			ServerSocket enregistrementserveur = new ServerSocket(nameNodeDataPort);
			EnregistreurDataNode enregistrementserveurtest = new EnregistreurDataNode(enregistrementserveur);
			Thread t = new Thread(enregistrementserveurtest);
			t.start();

		}catch (IOException e2){
			e2.printStackTrace();
		}

		/* On gére ici la communication entre le client et le NameNode. La communication peut être
		pour la localisation d'un fichier ou pour un accés pour lire/écrire/supprimer un fichier 
		Le NameNode répond avec une liste de DataNodes et sa fiche de présentation INoeud où sont trouver les blocs*/

		try {
			System.out.println("---------------------------------------");
			System.out.println("The NameNode is launched.");
			System.out.println("---------------------------------------");		
			ServerSocket communicationClientNameNode = new ServerSocket(NameNode.nameNodeClientPort);
			while (true) {
				Socket s = communicationClientNameNode.accept();
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
				String commande = (String) br.readLine();
				String[] commandes = commande.split("//");
				String nomfichier = commandes[1];
				
				
				
			/*
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			PrintWriter pw = new PrintWriter(s.getOutputStream(), true); 
			String commande = (String) br.readLine();
			String[] commandes = commande.split("//");
			String nomfichier = commandes[1];
			int facteur_duplication = Integer.parseInt(commandes[2]);
			int nombre_blocs = Integer.parseInt(commandes[3]); */
			/* Maintenant qu'on a reçu toutes les informations par rapport au fichier on traitera la requête selon la commande */
			if (commandes[0].equals("NBM")) {
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				oos.writeObject(machinesliste);	
				oos.close();
				s.close();
			} 
			if (commandes[0].equals("RD")) {
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				try {
				
					
				INoeud node = catalogue.get(nomfichier);
				
				
				oos.writeObject(node.getRepartitionFichier());
				} catch (NullPointerException e) {
					oos.writeObject("Le fichier que vous souhaitez lire n'existe pas dans nos serveur.");
				}
				oos.close();
				s.close();
				
			} 
			
			
			
			
			
			if (commandes[0].equals("RDM")) {
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				try {
				
					
				INoeud node = catalogue.get(nomfichier);
				
				
				oos.writeObject(node.getRepartitionFichier());
				} catch (NullPointerException e) {
					oos.writeObject("Le fichier que vous souhaitez lire n'existe pas dans nos serveur.");
				}
				oos.close();
				s.close();
				
			} 
			
			
			
			
			
			
			if (commandes[0].equals("DL")) {
				System.out.println("Le traitement de la requête delete a commencé");
				INoeud node = catalogue.get(nomfichier);
				if (node != null) {
					catalogue.remove(nomfichier);
				}
				s.close();
			}	
			/* Le but c'est envoyé la liste des machine où chaque bloc sera stocké */
			int j = 0;
			if (commandes[0].equals("WR")) {
				int facteur_duplication = Integer.parseInt(commandes[2]);
				int nombre_blocs = Integer.parseInt(commandes[3]);
				HashMap<Integer,ArrayList<String>> nouvellerepartionfichier = new HashMap<Integer,ArrayList<String>>();
				for (int i = 1;i<=nombre_blocs;i++) {
					/* On va choisir les DataNodes dans lesquels on va stocker les blocs */
					ArrayList<String> listemachines = new ArrayList<String>();
					/* On récupére les ports fonctionnel des machines */
					synchronized(NameNode.machinesliste){
						ArrayList<Integer> portfonctionnels = new ArrayList<Integer>();
						for (int portfonctionnel : NameNode.machinesliste.keySet()){
							portfonctionnels.add(portfonctionnel);
						}
						ArrayList<Integer> portschoisis = new ArrayList<Integer>();
						while (portschoisis.size() < facteur_duplication) {
							int port = portfonctionnels.get(j);
							portschoisis.add(port);
							j = (j+1)%portfonctionnels.size();
						}	
						for (int port : portschoisis) {
							String machineschoisis = machinesliste.get(port)+ "//" + Integer.toString(port);
							listemachines.add(machineschoisis);
						}
					}
					/* On fait le choix sur les machines pour stocker le bloc courant */
					nouvellerepartionfichier.put(i,listemachines);
				}
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				oos.writeObject(nouvellerepartionfichier);
				INoeud fichier_ecrit = new INoeud(nomfichier,nouvellerepartionfichier);
				NameNode.catalogue.put(nomfichier,fichier_ecrit);
				oos.close();
				br.close();
				pw.close();
				s.close();
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public void setnameNodeClientPort(int nameNodeClientPort2) {
		// TODO Auto-generated method stub;
		this.nameNodeClientPort=nameNodeClientPort2;
		
	}

	public void setnameNodeDataPort(int nameNodeDataPort2) {
		// TODO Auto-generated method stub
		this.nameNodeDataPort=nameNodeDataPort2;
	}

	public void setadresseNameNode(String adresse_namenode2) {
		// TODO Auto-generated method stub
		this.adresse_namenode=adresse_namenode2;
	}

	public void setdeamonmasterPort(int deamonmasterPort2) {
		// TODO Auto-generated method stub
		this.deamonmasterPort=deamonmasterPort2;
	}

	public String getDaemonmaseterrmiadress() {
		// TODO Auto-generated method stub
		InetAddress adresse;
		try {
			adresse = InetAddress.getLocalHost();
			NameNode.adresse_namenode = adresse.getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "//"+adresse_namenode+":" + deamonmasterPort + "/DaemonMaster";
	}

	public String getadresse_NameNode() {
		// TODO Auto-generated method stub
		return adresse_namenode;
	}

	public int getnameNodeClientPort() {
		// TODO Auto-generated method stub
		return nameNodeClientPort;
	}
}

