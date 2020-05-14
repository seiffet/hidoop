package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote{

	
	void waitmappers() throws RemoteException;

	void mapperterminer() throws RemoteException ;

}
