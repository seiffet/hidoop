package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;




public interface DaemonMaster extends Remote{
	
	public void addDaemoninfo(DataDaemoninfo info) throws RemoteException;
	
	public void addDaemonKeys(int daemonnum,ArrayList<String> daemkeyset) throws RemoteException;

	// n'est pas utilisé pour l'instant !!
	public ArrayList<String> getlistkeyfiles() throws RemoteException;
	
	public HashMap<String,ArrayList<String>> getkeyfiles() throws RemoteException;
	
	public ArrayList<String> getAllkeys() throws RemoteException ;

}
