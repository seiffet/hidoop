package ordo;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

//all right reserved FETTOUHI SEIFEDDIN CHADI
import application.MyMapReduce;
import config.Project;
import formats.Format;
import formats.FormatReader;
import formats.KV;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClientHelper;
import hdfs.NameNode;
import map.MapReduce;


public class Job implements JobInterfaceX {

	
	private String inputFname;
	private Type inputFormat;
	private String outputFname="testout000";
	private String mapOutFname="testmap000";
	private Type outputFormat=Format.Type.KV;

	private CallBack cb;
	public void setInputFname(String string) {
		// TODO Auto-generated method stub
		this.inputFname = string ;
	}

	public void setInputFormat(Type line) {
		// TODO Auto-generated method stub
		this.inputFormat = line ;
		
	}

	
	List<Daemon> listDaemon = new ArrayList<>();
	@Override
	public void startJob(MapReduce mr) {
		// TODO Auto-generated method stub
		
		Project p = new Project();
		
		//----------reader
		
		// n'est pas utilise pour l'instant donc on l'a commente
		/*Format nf = null ;
		switch (inputFormat) {
        case LINE:
           nf = new LineFormat(Project.PATH+"data/"+inputFname);
            
            

        case KV:
        	nf = new KVFormat(Project.PATH+"data/"+inputFname);
        }*/
		
		LineFormat nf = new LineFormat(Project.PATH+"/"+inputFname); //data
		nf.open(OpenMode.R);
		//----------writer
		KVFormat wr=new KVFormat(Project.PATH+"/"+mapOutFname); //data
		wr.open(OpenMode.W);
		
		//connection au DaemonMaster pour avoir trace et enregistrer les résultats du mappage !!
		// généralement il n'a pas d'utilité si on fait pas du shuffling !!
		
		
		//System.out.println("connection  DaemonMaster d'adresse : "+Project.Daemonmaseterrmiadress);

		DaemonMaster dm =null ;
		Project pp =new Project();
		NameNode nm = new NameNode(); 
		pp.buildNameNode(nm);
		//try {
			// dm = (DaemonMaster) Naming.lookup(nm.getDaemonmaseterrmiadress());
		//} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			// TODO Auto-generated catch block
		//	e1.printStackTrace();
		//}
		// 
		ArrayList<String> listmappersresultfiles = new ArrayList<>();
		
		
		int Daemonnum=Project.DaemonNum;
		// on fait appel hdfswrite pour répartir le fichier on plusieurs bloc
		

		long t1 = System.currentTimeMillis();
	
		System.out.println("Sending file to servers .... ");
						           //HdfsClientHelper.hdfswrite(Project.PATH+"/"+inputFname,"LINE",Project.facteurdedubplication,Project.facteurfragmentation); //data
		long t2 = System.currentTimeMillis();
		System.out.println("file sent succesfelly in "+(t2-t1)+"ms");
		// recevoire la liste des blocs et les machines ou ils situe !
		
		
		HashMap<Integer,ArrayList<String>> listbloc=HdfsClientHelper.getrepartitionfichier(Project.PATH+"/"+inputFname); //data
		
		
		
		// liste des machines connecté  
		
		HashMap<Integer,String> lmachines = HdfsClientHelper.getmachineoperationel();
		
		
		
		ArrayList<String> lidaem = new ArrayList<>();
		ArrayList<Integer> listport = new ArrayList<>();
 		// 1000+port de datanode chaque daemon est lancer dans le port 1000+le port du datanode
		
		
		for(Entry<Integer, String> machine : lmachines.entrySet()) {
			
			int port = 1000+machine.getKey() ;
			lidaem.add("//"+machine.getValue()+":"+(port)+"/Daemon")	;
			listport.add(port);
		}
		
		Project.setAdresslistDaemon(lidaem);
		Project.setPortlistDaemon(listport);
		Project.selectnumberofDaomon(lmachines.size());
		
		Daemonnum=listbloc.size();  // nombre de daemon qu'il faut pour faire le calcule 
		
		cb  =null;
		try {
			// callback avec le nombre de daemons pour pouvoir attendre la fin de calcule de toutes les daemons
			
			cb = new CallBackImpl(Daemonnum);
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		//Daemon daemn=null;
		t1 = System.currentTimeMillis();
		System.out.println("Request maping results from servers .... ");
		for (int i=0 ; i < Daemonnum;i++) {
			
			try {
				// HDFS
				
				Daemon daemn;
				
				//HDFS
				
				// 
				
				int firstmachine = Integer.parseInt((listbloc.get(i+1).get(0).split("//"))[1]);

				firstmachine-=8002;  // les ports des datanodes
				
				// donc c'est le nombre de machine a affecté a ce bloc
				
				// le cas de nom de bloc est le meme que nombre de machine cad facteur de duplication est = 1
				if(i<lmachines.size()) {
					
					//System.out.println("connection  Daemon d'adresse : "+Project.getAdresslistDaemon().get(i));
					daemn = (Daemon) Naming.lookup(Project.getAdresslistDaemon().get(i));
					listDaemon.add(daemn);
					
					
				}
				else {
					// sinon on fait le retour 
					
					daemn = listDaemon.get(firstmachine);
				}
				
				
				// commentaite ancien : partie hdfs il faut divisé le fichier inputfile est avoir des fichiers temporaires pour que chaque mapper traite une partie d fichiers !!
				
				
				
				
				//si pas de hdfs : String datatempfile = Project.PATH+"data/"+inputFname+"part"+i ;

				String datatempfile = Project.PATH+"/DataNode"+(firstmachine+1)+"/"+inputFname.split(".txt")[0]+"/BLOC"+(i+1)+".hdfs" ; // on élémine le .txt
				
				// if faut faire un test de format ici on cas d'un fichier Kv !
				
				Format inputfileparti = new LineFormat(datatempfile);
				
				//si pas de hdfs : String outpmappertempfile = Project.PATH+"data/"+inputFname+"maptemp"+i ;
				
				String outpmappertempfile = Project.PATH+"/DataNode"+(firstmachine+1)+"/"+inputFname.split(".txt")[0]+"/BLOC"+(i+1)+".map" ; // on élémine le .txt

				
				//DataDaemoninfo ddinf=new DataDaemoninfo("localhost", Project.getPortlistDaemon().get(i), Project.getAdresslistDaemon().get(i));
				
				

//###########################################################################################"""
				// enregistre les info dans un daemon master comme namenode pour avoir trace de calcule pour le daemon
				DataDaemoninfo ddinf=new DataDaemoninfo(lmachines.get(firstmachine), Project.getPortlistDaemon().get(firstmachine), Project.getAdresslistDaemon().get(firstmachine));
				ddinf.setDatatempfile(datatempfile);
				ddinf.setMappertempfile(outpmappertempfile);
				// utile dans une autres version 
				ddinf.setFirstInputfile(Project.PATH+"data/"+inputFname);
				//dm.addDaemoninfo(ddinf);
				// mais pas pour l'instant !
//####################################################################################################"		
				
				
				//fin partie hdfs
				
				
				
				
				// les listes des fichies mappers généré si on veut les supprimer apres !
				
				listmappersresultfiles.add(outpmappertempfile);
				
				
				
				Format tmppartmaperi = new KVFormat(outpmappertempfile);
				
				
				// lancer les mappers !! 
				
				Thread thmap = new Thread(new Runnable() {
					
					@Override
					public void run() {
						
						try {
							daemn.runMap(mr, inputfileparti, tmppartmaperi, cb);
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				//System.out.println("Daemon start mapping :"+Project.getAdresslistDaemon().get(firstmachine));
				thmap.start();
				
				
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		// n'est pas utilisé a traité avec parte HDFS !
		
		nf.close();
		wr.close();
		
		// attendre tous les mappers
		
		try {
			cb.waitmappers();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		t2 = System.currentTimeMillis();
		System.out.println("mapping finished in "+(t2-t1)+" ms");
		t1 = System.currentTimeMillis();
		System.out.println("Starting reducing with methode "+Project.reduceMethod);
		// 3 possibilé de reduce
		// 2 : une avec du shuflling : pas optimale 
		// 1 : reduce récursive testé en local sans hdfs , dans l'attent d'un hdfs qui peut lire qu'une partie des blocs
		switch(Project.reduceMethod) {
		case 1 :
		try {
			String outputreduce=reducerecursive(mr, Daemonnum, Project.PATH+"data/"+inputFname+"maptemp",new CallBackImpl(Daemonnum/2));
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			break;
		case 0 : 
		
		// rassemblé les blocs
			
		HdfsClientHelper.hdfsreadmap(Project.PATH+"/"+inputFname,"KV",Project.PATH+"/"+inputFname+"-resmap");   ///data
	
			
		Format finaloutput=new KVFormat(Project.PATH+"/"+inputFname+"-resmap"); //data
	
		// code sans hdfs 
		/*finaloutput.open(OpenMode.W);
		KV kv ;
		for(int i=0 ; i < Daemonnum ; i++) {
			System.out.println("reducer ==>" + i);
			
			Format reduceres=new KVFormat(Project.PATH+"data/"+inputFname+"maptemp"+i);
			reduceres.open(OpenMode.R);
			while((kv = reduceres.read()) != null)
			{
				finaloutput.write(kv);
			}
			
			
			reduceres.close();
		}
		finaloutput.close();*/
		
		finaloutput.open(OpenMode.R);
		
		
		Format finalresult=new KVFormat(Project.PATH+"/"+inputFname+"-res");  // data
		
		finalresult.open(OpenMode.W);
		
		// reduce finale du fichiers rassemblé par le mapper pour avoir les résultats
		
		mr.reduce(finaloutput, finalresult);
		
		
		finaloutput.close();
		finalresult.close();
		break;
		
		
		case 2 :
			
		// va pas fonctionné il fonctioné il faut décommenté la partie shuffling du mappers
			
		HashMap<String, ArrayList<String>> keymap=null;
		ArrayList<String> keylist = null;
		
		// il faut  maintenant rassembler les résultats de chaque mapper dans un seul endroie (pour les meme clé !!)
		// d'où l'interet de Daemonmaster
		//try {
		//	keymap = dm.getkeyfiles();
			//keylist= dm.getAllkeys();
		//} catch (RemoteException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
		// dans un premier temp on prend le meme nombre de reducers que les mappers !!
		
		
		int numberofkeysforeachreducer = keylist.size()/Daemonnum;
		
		
		
		HashMap<String, ArrayList<Format>> reducemap0 = new HashMap<>();
		
		//System.out.println("keylist"+keylist);
		
		// pour faire reduce du reste de la list !!
		
		ArrayList<String> daemonikeys = new ArrayList<String>(keylist.subList(0*numberofkeysforeachreducer, 1*numberofkeysforeachreducer));
	
		daemonikeys.addAll(new ArrayList<String>(keylist.subList((Daemonnum-1)*numberofkeysforeachreducer, keylist.size())));
		
		
		//HashMap<String, Format> outputreducekeyformats0 = new HashMap();
		Format outputreducerformat0= new KVFormat(Project.PATH+"data/"+inputFname+"reduceof"+0);
		for(String key : daemonikeys) {
			
			ArrayList<String> shuflefilenames = keymap.get(key);
			
			ArrayList<Format> shufleformatnames = new ArrayList<>();
			for(String filename : shuflefilenames) {
				
				Format keymapperfileformat = new KVFormat(filename);
				shufleformatnames.add(keymapperfileformat);
			}
			
			reducemap0.put(key, shufleformatnames);
			//outputreducekeyformats0.put(key,new KVFormat(Project.PATH+"data/"+inputFname+"reduceof"+key));
		
		}
		Thread threduce = new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					listDaemon.get(0).runReduce(mr, reducemap0, outputreducerformat0, cb);
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		System.out.println("reducer "+0+" started");
		threduce.start();
		
		for(int i=1 ; i < Daemonnum ; i++)
		{
			HashMap<String, ArrayList<Format>>  reducemap = new HashMap<>();
			Daemon d = listDaemon.get(i);
			
			daemonikeys = new ArrayList<String>(keylist.subList(i*numberofkeysforeachreducer, (i+1)*numberofkeysforeachreducer));
			
			//System.out.println(" list i"+daemonikeys);
			//HashMap<String, Format> outputreducekeyformats = new HashMap<>();
			Format outputreducerformat = new KVFormat(Project.PATH+"data/"+inputFname+"reduceof"+i);
			for(String key : daemonikeys) {
				
				ArrayList<String> shuflefilenames = keymap.get(key);
				ArrayList<Format> shufleformatnames = new ArrayList<>();
				for(String filename : shuflefilenames) {
					
					Format keymapperfileformat = new KVFormat(filename);
					shufleformatnames.add(keymapperfileformat);
				}
				
				reducemap.put(key, shufleformatnames);
				// Dans un premier temp j'ai enregistrer le résultat de chaque Key dans un fichier mais on peut tous simplement enregiter toutes le résltats dans un seul fichiers , le but c'étais d'encore mieu parallélisé le calcule mais cela ne peut pas etre le cas vu le nombre de fichiers générer   
				
				// outputreducekeyformats.put(key,new KVFormat(Project.PATH+"data/"+inputFname+"reduceof"+key));
				
			}
			threduce = new Thread(new Runnable() {
					
					@Override
					public void run() {
						
						try {
							d.runReduce(mr, reducemap, outputreducerformat, cb);
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				
				threduce.start();
				
				System.out.println("reducer "+i+" started");
		
		
		}
		
		
		System.out.println("waiting reducers ....");

	
		try {
			cb.waitmappers();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		t2 = System.currentTimeMillis();
		System.out.println("reduce finished in "+(t2-t1)+"ms");
		
		// ressemblé dans un seul fichier
		// just pour testé sans le HDFS !!!
		
		Format finalout=new KVFormat(Project.PATH+"data/"+inputFname+"-res");
		finalout.open(OpenMode.W);
		KV kv1 ;
		for(int i=0 ; i < Daemonnum ; i++) {
			System.out.println("reducer ==>" + i);
			Format reduceres=new KVFormat(Project.PATH+"data/"+inputFname+"reduceof"+i);
			reduceres.open(OpenMode.R);
			while((kv1 = reduceres.read()) != null)
			{
				finalout.write(kv1);
			}
			
			
			reduceres.close();
		}
		finalout.close();
		break;
		}
		System.out.println("MapReduce finished : merci pour votre patience tous drois résevé au groupe : FETTOUHI et CHADI !!");
	}

	
	public String reducerecursive(MapReduce mr,int Daemonnum,String filename,CallBack cb) throws RemoteException {
		
		if(Daemonnum != 1) {
			
		String filefin;
		int numberofresreduced = 0;
		int rest = Daemonnum % 2 ;
		int numres = 0;
		
		
		while (numberofresreduced <= Daemonnum -1 - rest) {
			Daemon d = listDaemon.get(numberofresreduced) ;
			Format reduceres1=new KVFormat(filename+numberofresreduced++);
			Format reduceres2=new KVFormat(filename+numberofresreduced++);
			if (cb == null) System.out.println("cb is empty !!");
			System.out.println("cb is  not empty !!");
			Thread thred = new Thread(new Runnable() {
				
				@Override
				public void run() {
					
					try {
						d.runReduce1(mr, reduceres1,reduceres2,new KVFormat(filename+"reduce"+numres),cb);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			thred.start();
			
		}
		try {
			cb.waitmappers();
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		filefin=reducerecursive(mr,numres+1,filename+"reduce",new CallBackImpl((numres+1)/2));
		if(rest == 1) {
			Format reduceres=new KVFormat(filefin);
			Format reducerest=new KVFormat(filename+(Daemonnum-1));
			filefin = filename+"finale" ;
			try {
				CallBack cbr = new CallBackImpl(1); 
				listDaemon.get(0).runReduce1(mr, reduceres,reducerest,new KVFormat(filefin),cbr);
				cbr.waitmappers();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			return filefin;
		}else {
			return filename+0;
		}
		
	}
	@Override
	public void setNumberOfReduces(int tasks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOutputFormat(Type ft) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOutputFname(String fname) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNumberOfReduces() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumberOfMaps() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Type getInputFormat() {
		// TODO Auto-generated method stub
		return inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		// TODO Auto-generated method stub
		return outputFormat;
	}

	@Override
	public String getInputFname() {
		// TODO Auto-generated method stub
		return inputFname;
	}

	@Override
	public String getOutputFname() {
		// TODO Auto-generated method stub
		return outputFname;
	}

	@Override
	public SortComparator getSortComparator() {
		// TODO Auto-generated method stub
		return null;
	}

}
