package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;


/* Cette classe contient les informations du fichier. La carte d'identité des fichiers */ 

public class INoeud implements Serializable {

	private String nomfichier;
	private int version;

	/* Chaque serveur DataNode gére plusieurs copie de fichier et donc on a besoin 
	de cette structure de donnée qui associer l'identifiant du bloc avec les noms des serveurs 
	contenant les copie de ce bloc */
	private HashMap<Integer,ArrayList<String>> repartitionfichier;

	public INoeud (String nomfichier, HashMap<Integer,ArrayList<String>> repartitionfichier) {
		this.nomfichier = nomfichier;
		this.repartitionfichier = repartitionfichier;
		this.version = 1;
	}

	/* Les getteurs et les setteurs */

	public String getNomfichier() {
		return nomfichier;
	}

	public void setNomfichier(String filename) {
		this.nomfichier = nomfichier;
	}

	public int getVersion() {
		return version;
	}

	public HashMap<Integer, ArrayList<String>> getRepartitionFichier() {
		return this.repartitionfichier;
	}
	
	

}