package hdfs;

import java.io.File;
import java.util.Scanner;


public class HdfsClient {

	public static void main(String[] args) {
		System.out.println("---------------------------------------");
		System.out.println("Bienvenue! Ceci est l'interface d'HDFS.");
		System.out.println("---------------------------------------");
		Scanner sc1 = new Scanner(System.in);
		System.out.println("Veuillez choisir le facteur de duplication des blocs dans les datanodes. ");
		int facteur_duplication = sc1.nextInt();
		System.out.println("Veuillez choisir votre mode de fragmentation. 128Mo ou 64Mo ou 400 octets ?");
		int facteur_fragmentation = sc1.nextInt();

		do {
			System.out.println("---------------------------------------");
			System.out.println("Bienvenue à l'univers d'HDFS. On attend vos commandes!! :) ");
			System.out.println("---------------------------------------");
			Scanner sc2 = new Scanner(System.in);
			System.out.println("Veuillez saisir le nom du fichier.");
			String nomfichier = sc2.nextLine();
			System.out.println(" Quel est son format ?");
			String formatfichier = sc2.nextLine();
	
			try {
				System.out.println("---------------------------------------");
				System.out.println(" Vous pouvez écrire avec la commande : WR, lire avec RD ou bien supprimer avec DL ");
				System.out.println("---------------------------------------");
				System.out.println(" Veuillez saisir votre commande.");
				System.out.println("---------------------------------------");
				String commande = sc2.nextLine();
				if (commande.equals("WR")) {
					if (new File(nomfichier).exists()) {
						HdfsClientHelper.hdfswrite(nomfichier,formatfichier,facteur_duplication,facteur_fragmentation);
					}else {
						System.out.println("Erreur! Fichier inexistant.");
					}
					
				}else if (commande.equals("RD")) {
					System.out.println("---------------------------------------");
					System.out.println(" Saisissez le nom du fichier où votre fichier sera stocker.");
					System.out.println("---------------------------------------");
					String nomfichierdestination = sc2.nextLine();
					HdfsClientHelper.hdfsread(nomfichier,formatfichier,nomfichierdestination);
				/*} else if (commande = "DL") {
					HdfsClientHelper.hdfsdelete(nomfichier,formatfichier,facteur_duplication,facteur_fragmentation);
				}*/
				} else {
					System.out.println("---------------------------------------");
					System.out.println("Vous voulez vraiment supprimer " + nomfichier + " ? (O/N)");
					System.out.println("---------------------------------------");
					String reponse = sc2.nextLine();
					if (reponse.equals("O")) {
						HdfsClientHelper.hdfsdelete(nomfichier);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}while (true);
	}
}