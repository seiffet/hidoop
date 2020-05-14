package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.Format.OpenMode;
import java.net.UnknownHostException;
import java.net.InetAddress;
//import jdk.internal.module.IllegalAccessLogger.Mode;
import map.Mapper;
import map.Reducer;
import hdfs.NameNode;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	private int daemonnum;

	public DaemonImpl(int daemonnum2) throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
		
		daemonnum = daemonnum2;
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		// TODO Auto-generated method stub
		
		
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);
		
		// mapping de fichier donnée
		//System.out.println("mapping ...");
		m.map(reader, writer);
		//System.out.println("fin mapping!");
		
		
		reader.close();
		writer.close();
		
		
		//shuffling
		
		if(Project.reduceMethod == 2) {
			
		// lire le fichier mapper et répartire selon le clé !!
		KV kv;
		writer.open(OpenMode.R);
		
		// lecture et ecriture temporaire !!
		
		// ici on récupère toutes les key trouvé  dans une list et leurs valeurs dans le hashmap pour avoir trace des key mapper par ce daemon on cas de besoin
		HashMap<String, ArrayList<String>> mapkeys = new HashMap<String, ArrayList<String>>();
		ArrayList<String> listkeys = new ArrayList<>();
		while((kv = writer.read()) != null)
		{
			if(mapkeys .containsKey(kv.k))
			{
				 mapkeys.get(kv.k).add(kv.v);
			}
			else {
				
				listkeys.add(kv.k);
				ArrayList<String> vlist = new ArrayList<String>();
				vlist.add(kv.v);
				mapkeys.put(kv.k,vlist);
			}
		}
		writer.close();
		
		// pour chaque key on l'enregiste dans dans un fichier tempkey pour pouvoir le recupérer séparement des autre key
		for (String key : mapkeys.keySet())
		{
			// dans ce cas on enregistre dans le meme repertoire que le fichier d'entrer
			
			// HDFS !!!
			KVFormat keywriter = new KVFormat(writer.getFname()+"key"+key);   //tempmapperfilepartikeya
			
			keywriter.open(Format.OpenMode.W);
			
			for(String val: mapkeys.get(key))
			{
				KV kv1 = new KV(key,val);
				
				keywriter.write(kv1);	
			}
			keywriter.close();
		}
		//monmaseterrmiadr
		

		
		
		
		// envoyer trace de key shufler dans ce daemons!
		
		DaemonMaster dM =null;
		 try {

        Project pp =new Project();
		NameNode nm = new NameNode(); 
		pp.buildNameNode(nm);
			// dM = (DaemonMaster) Naming.lookup(nm.getDaemonmaseterrmiadress());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		//System.out.println("SEND TRACE TO DAEMONMASTER !!");
		ArrayList<String> daemkeyset=new ArrayList<>();
		for(String key : mapkeys.keySet()) {
			daemkeyset.add(key);
			
			
		}
		// normalement il aura mieu d'envoyer cette liste dans une socket vu son potentiel grand taille
		//dM.addDaemonKeys(daemonnum,daemkeyset);
		
		}
		//System.out.println("fin shufling !!");
		
		cb.mapperterminer();

		
		
	}

	
	
	public void start_daemon(int port) {
		
		
		
		Registry registry =null;
		try {
			 LocateRegistry.createRegistry(port);
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			
			
			System.out.println("Daemon started in port  "+port+"");

		   	InetAddress adresse;
            try {
            adresse = InetAddress.getLocalHost();
            Naming.rebind("//"+adresse.getHostAddress()+":"+port+"/Daemon",this);
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block                   
                e.printStackTrace();
            }
            
			
			//Naming.rebind("//localhost:"+port+"/Daemon",this);

		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
	}
	
	
	// main non utilisé  !!
	public static void main(String args[]) {
		int port = Integer.parseInt(args[0]);
		
		int daemonnum = Integer.parseInt(args[1]);
		Registry registry =null;
		try {
			 LocateRegistry.createRegistry(port);
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			
			
			//System.out.println("Daemon started //localhost: "+(args[0])+"/Daemon" +args[1]);
			System.out.println("Daemon started //localhost: "+(args[0])+"/Daemon");

		
			//Naming.rebind("//localhost:"+(args[0])+"/Daemon" +args[1],new DaemonImpl(daemonnum));
			
			Naming.rebind("//localhost:"+(args[0])+"/Daemon",new DaemonImpl(daemonnum));

		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
		
	}

	@Override
	public void runReduce(Reducer m, HashMap<String, ArrayList<Format>> reducemap,/*HashMap<String, Format> outputreducekeyformats*/
			Format outputreducerformat, CallBack cb) throws RemoteException {
		// TODO Auto-generated method stub
		
		// ici on fait le reducing par key chaque daemon prend une liste de key les rasemble dans un seul fichier puis il fait le reduce
		
		System.out.println("reducer started " + reducemap.keySet());
		
		
		
		Format tempwriter=new KVFormat(outputreducerformat.getFname()+"temp");
		tempwriter.open(OpenMode.W);
		
		
		System.out.println("loading reducer"+daemonnum+" key set");
		// partie Hdfs !!
		for (String key : reducemap.keySet()) {
			
			/*Format ouputformat=outputreducekeyformats.get(key);*/
			
			for(Format keyformatparti : reducemap.get(key)) {
				keyformatparti.open(OpenMode.R);
				KV kv ;
				while((kv = keyformatparti.read()) != null)
				{
						tempwriter.write(kv);
				}
				keyformatparti.close();	
			}
			
		}
				
		tempwriter.close();
		tempwriter.open(OpenMode.R);
		outputreducerformat.open(OpenMode.W);
		
		m.reduce(tempwriter, outputreducerformat);
		tempwriter.close();
		outputreducerformat.close();
		
		
		// les meme machines donc meme nombre 
		cb.mapperterminer();
		System.out.println("fin reducing!");
		
	}

	@Override
	public void runReduce1(Reducer m, Format reducefile1, Format reducefile2, Format reduceres , CallBack cb) throws RemoteException {
		// TODO Auto-generated method stub
		
		reducefile1.open(OpenMode.R);
		reducefile2.open(OpenMode.WA);
		
		reduceres.open(OpenMode.W);
		
		m.reduce(reducefile1, reducefile2);
		
		reducefile2.close();
		reducefile2.open(OpenMode.R);
		m.reduce(reducefile2, reduceres);
		
		reducefile1.close();
		reduceres.close();
		
		cb.mapperterminer();
	}

}
