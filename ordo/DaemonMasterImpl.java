package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.net.InetAddress;
import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;



public class DaemonMasterImpl extends UnicastRemoteObject implements DaemonMaster {

	protected DaemonMasterImpl() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
		
		keysofDaemons = new HashMap<>();
		Daemons = new ArrayList<>();
		allkeys = new ArrayList<>();
	}

	// n'est pas utilisé !
	public void setKeysofDaemons(HashMap<DataDaemoninfo, ArrayList<String>> keysofDaemons) throws RemoteException {
		this.keysofDaemons = keysofDaemons;
	}

	HashMap<DataDaemoninfo,ArrayList<String>> keysofDaemons;
	ArrayList<DataDaemoninfo> Daemons;
	private ArrayList<String> allkeys;
	
	
	public ArrayList<String> getAllkeys() throws RemoteException {
		return allkeys;
	}
	// retourn une map de key et les keyfile de chaque map machine !!
	public HashMap<String,ArrayList<String>> getkeyfiles() throws RemoteException {
		
		System.out.println("sendkeyfiles");
		System.out.println("test of"+keysofDaemons);
		allkeys = new ArrayList<>();
		HashMap<String,ArrayList<String>> mapkey = new HashMap<String,ArrayList<String>>();
		
		for (DataDaemoninfo inf : keysofDaemons.keySet()) {
			
			for(String key : keysofDaemons.get(inf)) {
				if(!mapkey.containsKey(key)) {
					
					ArrayList<String> keyfiles = new ArrayList<>();
					keyfiles.add(inf.getMappertempfile()+"key"+key);
					mapkey.put(key,keyfiles);
					allkeys.add(key);
					
				}else {
					
					// ajouter tous les files de meme key !!
					mapkey.get(key).add(inf.getMappertempfile()+"key"+key);
					
				}
				
			
			}}
		System.out.println("keyfiles sent");

		return mapkey;
			
		
		
	}
	@Override
	public ArrayList<String> getlistkeyfiles() throws RemoteException {
		// TODO Auto-generated method stub
		
		ArrayList<String> keyList = new ArrayList<>();
		
		for (DataDaemoninfo inf : keysofDaemons.keySet()) {
			
			for(String key : keysofDaemons.get(inf)) {
				
				if(!keyList.contains(key)) 
					 {
					keyList.add(key);
				}
				Format rd = new KVFormat (inf.getMappertempfile());
				rd.open(OpenMode.R);
				
				Format wrkey = new KVFormat (DataDaemoninfo.getfirstInputfile()+key);
				wrkey.open(OpenMode.W);
				
				KV kv;
				while((kv = rd.read()) != null)
				{
					wrkey.write(kv);	
				}
				wrkey.close();
			}
			
		}
		
		return keyList;
	}

	@Override
	public void addDaemoninfo(DataDaemoninfo info) throws RemoteException {
		// TODO Auto-generated method stub
		Daemons.add(info);
		
	}

	@Override
	public void addDaemonKeys(int daemonnum, ArrayList<String> keys) throws RemoteException {
		// TODO Auto-generated method stub
		keysofDaemons.put(Daemons.get(daemonnum), keys);
		System.out.println("list of keys add");
	}
	
	public static void startdemonmaster(int port) {

		System.out.println("port "+port);

        

            InetAddress adresse;
            try {
            adresse = InetAddress.getLocalHost();


            DaemonMasterImpl dm = new DaemonMasterImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//"+adresse.getHostAddress()+":" + port + "/DaemonMaster", dm);
			

            } catch (Exception e) {
                // TODO Auto-generated catch block                   
                e.printStackTrace();
            }



		/*try {
			DaemonMasterImpl dm = new DaemonMasterImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//"+adresse.getHostAddress()+":" + port + "/DaemonMaster", dm);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}*/
	}
	
	public static void main(String args[]) {
		int port = Integer.parseInt(args[0]);
		
		if(args.length > 1) {
			
			Project.selectreduceMethod(Integer.parseInt(args[1]));
		}
		if(args.length > 2) {
			
			Project.selectnumberofDaomon(Integer.parseInt(args[2]));
		}
		try {
			

			DaemonMasterImpl dm = new DaemonMasterImpl();
			LocateRegistry.createRegistry(port);
			Naming.rebind("//localhost:" + port + "/DaemonMaster", dm);
			
		} catch (RemoteException | MalformedURLException e) {
			e.printStackTrace();
		}
		
	}

}
