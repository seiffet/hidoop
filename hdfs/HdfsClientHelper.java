package hdfs;


import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

public class HdfsClientHelper {

	/* la classe qui gére les méthodes utilisées par le client pour écrire,lire ou supprimer un fichier utilisé par le systéme HDFS */

	public static HashMap<Integer,ArrayList<String>> getlisteblocsmachines (String nomfichier, int facteur_duplication, int nombre_blocs) {

		HashMap<Integer,ArrayList<String>> listeblocsmachines = new HashMap<Integer,ArrayList<String>>();
		try {
		Project pp =new Project();
		NameNode nm = new NameNode(); 
		pp.buildNameNode(nm);
		Socket clientNameNode = new Socket(nm.getadresse_NameNode(),nm.getnameNodeClientPort());
		PrintWriter pw = new PrintWriter(clientNameNode.getOutputStream(), true);
		pw.println("WR" + "//" + nomfichier + "//" + facteur_duplication + "//" + nombre_blocs);
		ObjectInputStream ois = new ObjectInputStream(clientNameNode.getInputStream());
		listeblocsmachines = (HashMap<Integer,ArrayList<String>>) ois.readObject();
		pw.close();
		clientNameNode.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return listeblocsmachines;
	}

	public static HashMap<Integer, String> getmachineoperationel() {

		HashMap<Integer, String> machinesoperationel = new HashMap<Integer, String>();
		try {
			Project pp =new Project();
			NameNode nm = new NameNode(); 
			pp.buildNameNode(nm);
			Socket clientNameNode = new Socket(nm.getadresse_NameNode(),nm.getnameNodeClientPort());
			PrintWriter pw = new PrintWriter(clientNameNode.getOutputStream(), true);
			pw.println("NBM" + "//" + " ");
			ObjectInputStream ois = new ObjectInputStream(clientNameNode.getInputStream());
			machinesoperationel = (HashMap<Integer, String>) ois.readObject();
			pw.close();
			clientNameNode.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return machinesoperationel;
	}

	public static HashMap<Integer,ArrayList<String>> getrepartitionfichier (String nomfichier) {

		HashMap<Integer,ArrayList<String>> repartitionfichier = new HashMap<Integer,ArrayList<String>>();
		try {
			Project pp =new Project();
			NameNode nm = new NameNode(); 
			pp.buildNameNode(nm);
			Socket clientNameNode = new Socket(nm.getadresse_NameNode(),nm.getnameNodeClientPort());
			PrintWriter pw = new PrintWriter(clientNameNode.getOutputStream(), true);
			pw.println("RD" + "//" + nomfichier);
			ObjectInputStream ois = new ObjectInputStream(clientNameNode.getInputStream());
			repartitionfichier = (HashMap<Integer,ArrayList<String>>) ois.readObject();
			pw.close();
			
			clientNameNode.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return repartitionfichier;
	}

	
	public static void hdfswrite(String nomfichier, String formatfichiertype, int facteur_duplication, int facteur_fragmentation) {
		Format fichiersource;
		File filesource = new File(nomfichier);
		if (formatfichiertype.equals("LINE")) {
			fichiersource = new LineFormat(nomfichier);
		} else {
			fichiersource = new KVFormat(nomfichier);
		}
		/* J'ajoute 1 au nombres de blocs pour prendre en compte le fait que c'ets une devision euclédienne et il faut tenir encore du reste qui doit être considérer comme bloc */
		long nombre_blocs;
		Long ff;
		
		if (facteur_fragmentation == 128) {
			//nombre_blocs = ((new File(nomfichier).length())/128000000L);
			ff = 1024000000L;
			nombre_blocs = 0L;
			fichiersource.open(Format.OpenMode.R);
			long index = 0L;
			long index_aux = 0L;
			while (index < filesource.length()) {
			/*nombre_blocs = ((new File(nomfichier).length())/400L);*/
				if (formatfichiertype.equals("LINE")) {
					try {
						KV ligne = fichiersource.read();
						index = index + ligne.v.length()*8;
						if (index_aux < ff) {
							index_aux += ligne.v.length()*8 ;
							continue;
						} else {
							nombre_blocs = nombre_blocs + 1;
							index_aux = 0L;
						}
						
					}catch (NullPointerException n) {
						fichiersource.close();
						return;
					}catch (Exception e) {
						e.printStackTrace();
					}
				}else {
					try {
						KV clevaleur = fichiersource.read();
						index = index + (clevaleur.v.length()+clevaleur.k.length()+KV.SEPARATOR.length())*8;
						if (index_aux < ff) {
							index_aux += (clevaleur.v.length()+clevaleur.k.length()+KV.SEPARATOR.length())*8; ;
							continue;
						} else {
							nombre_blocs = nombre_blocs + 1;
						}
					}catch (NullPointerException n) {
						fichiersource.close();
						return;
					}catch (Exception e) {
							e.printStackTrace();
					}
				}
		}
		
		} else if (facteur_fragmentation == 64) {
			//nombre_blocs = ((new File(nomfichier).length())/64000000L);
			ff = 512000000L;
			nombre_blocs = 1L;
			fichiersource.open(Format.OpenMode.R);
			long index = 0L;
			long index_aux = 0L;
			KV ligne ;
			while ((ligne = fichiersource.read()) != null) {
				/*nombre_blocs = ((new File(nomfichier).length())/400L);*/
					if (formatfichiertype.equals("LINE")) {
						try {
							//KV ligne = fichiersource.read();
							
							//index = index + ligne.v.length()*8;					
							if (index_aux < ff) {
						
								index_aux += (ligne.v.length()-1)*8  ;
								//continue;
							} else {
								
								nombre_blocs = nombre_blocs + 1;
								
								index_aux = 0L;
							
							}
							
						}catch (NullPointerException n) {
						
							fichiersource.close();
							//return;
						}catch (Exception e) {
							
							e.printStackTrace();
						}
					}else {
						try {
							KV clevaleur = fichiersource.read();
							index = index + (clevaleur.v.length()+clevaleur.k.length()+KV.SEPARATOR.length())*8;
							if (index_aux < ff) {
								
								continue;
							} else {
								nombre_blocs = nombre_blocs + 1;
							}
						}catch (NullPointerException n) {
							fichiersource.close();
							return;
						}catch (Exception e) {
							
								e.printStackTrace();
						}
					}
					
			}
		
		} else {
			ff = 3200L;
			nombre_blocs = 1L;
			fichiersource.open(Format.OpenMode.R);
			long index = 0L;
			long index_aux = 0L;
			KV ligne ;
			while ((ligne = fichiersource.read()) != null) {
				/*nombre_blocs = ((new File(nomfichier).length())/400L);*/
					if (formatfichiertype.equals("LINE")) {
						try {
							//KV ligne = fichiersource.read();
							
							index = index + ligne.v.length()*8;					
							if (index_aux < ff) {
						
								index_aux += ligne.v.length()*8 ;
								//continue;
							} else {
								
								nombre_blocs = nombre_blocs + 1;
								
								index_aux = 0L;
							
							}
							
						}catch (NullPointerException n) {
						
							fichiersource.close();
							//return;
						}catch (Exception e) {
							
							e.printStackTrace();
						}
					}else {
						try {
							KV clevaleur = fichiersource.read();
							index = index + (clevaleur.v.length()+clevaleur.k.length()+KV.SEPARATOR.length())*8;
							if (index_aux < ff) {
								
								continue;
							} else {
								nombre_blocs = nombre_blocs + 1;
							}
						}catch (NullPointerException n) {
							fichiersource.close();
							return;
						}catch (Exception e) {
							
								e.printStackTrace();
						}
					}
					
			}
			
		}
		
		/* On a besoin de savoir la liste des machines dans lesquelles on va stocker les différents fragments du fichier */
		HashMap<Integer,ArrayList<String>> listeblocsmachines = getlisteblocsmachines (nomfichier,facteur_duplication,(int) nombre_blocs);
		
		/* On a obtenue la liste de liste de machine où chaque blocs sera stocké */
	
		try {

			ThreadWrite threadrespoecriture = new ThreadWrite(fichiersource, formatfichiertype, listeblocsmachines, ff, (int) nombre_blocs);
		
			Thread t = new Thread(threadrespoecriture);
			long t1 = System.currentTimeMillis();
			t.start();
			t.join();
			long t2 = System.currentTimeMillis();
			System.out.println("filefile "+(t2-t1)+" ms ");
			System.out.println("---------------------------------------");
			System.out.println("Génial! votre fichier est écrit dans nos serveurs.");
			System.out.println("---------------------------------------");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void hdfsread(String nomfichier, String formatfichiertype, String nomfichierdestination) {
		Format fichierdestination;
		File filedestination = new File(nomfichierdestination);
		if (formatfichiertype.equals("LINE")) {
			fichierdestination = new LineFormat(nomfichierdestination);
		} else {
			fichierdestination = new KVFormat(nomfichierdestination);
		}

		HashMap<Integer,ArrayList<String>> repartitionfichier = getrepartitionfichier(nomfichier);

		try {
			ThreadRead threadrespolecture = new ThreadRead(fichierdestination, nomfichier,formatfichiertype, repartitionfichier);
			Thread t = new Thread(threadrespolecture);
			t.start();
			System.out.println("---------------------------------------");
			System.out.println("Génial! Le fichier a été lu. Vous pouvez l'ouvrir avec le nom que vous avez donné : " + nomfichierdestination);
			System.out.println("---------------------------------------");
		}catch( Exception e) {
			e.printStackTrace();
		}
	}

	
	public static void hdfsreadmap(String nomfichier, String formatfichiertype, String nomfichierdestination) {
		Format fichierdestination;
		File filedestination = new File(nomfichierdestination);
		if (formatfichiertype.equals("LINE")) {
			fichierdestination = new LineFormat(nomfichierdestination);
		} else {
			fichierdestination = new KVFormat(nomfichierdestination);
		}

		HashMap<Integer,ArrayList<String>> repartitionfichier = getrepartitionfichier(nomfichier);
	

		try {
			ThreadReadMap threadrespolecture = new ThreadReadMap(fichierdestination, nomfichier,formatfichiertype, repartitionfichier);
			Thread t = new Thread(threadrespolecture);
			t.start();
			
			// pour testé apres il faut faire une syncronisation !
			t.join();
			System.out.println("---------------------------------------");
			System.out.println("Génial! Le fichier a été lu. Vous pouvez l'ouvrir avec le nom que vous avez donné : " + nomfichierdestination);
			System.out.println("---------------------------------------");
		}catch( Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void hdfsdelete(String nomfichier) {
		try {
			HashMap<Integer,ArrayList<String>> repartitionfichier = getrepartitionfichier(nomfichier);
			Socket clientNameNode = new Socket(InetAddress.getByName(NameNode.adresse_namenode),8000);
			PrintWriter pw = new PrintWriter(clientNameNode.getOutputStream(), true);
			pw.println("DL" + "//" + nomfichier);
			pw.close();
			clientNameNode.close();
			for (int bloc : repartitionfichier.keySet()) {
				ThreadDelete threadrespodelete = new ThreadDelete(bloc,repartitionfichier.get(bloc),nomfichier);
				Thread t = new Thread(threadrespodelete);
				t.start();
				System.out.println("---------------------------------------");
				System.out.println("Votre fichier a quitté nos serveurs :( ");
				System.out.println("---------------------------------------");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
