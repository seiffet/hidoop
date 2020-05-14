package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

import config.Project;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

	
		
	private int DaemonNum;
	private  Semaphore Mappers;
	public CallBackImpl()  throws RemoteException  {
		// TODO Auto-generated constructor stub
		super();
		DaemonNum=Project.DaemonNum;
		Mappers = new Semaphore(0);
	}
	public CallBackImpl(int i) throws RemoteException {
		// TODO Auto-generated constructor stub
		super();
		DaemonNum=i;
		Mappers = new Semaphore(0);
	}
	@Override
	public void waitmappers() throws RemoteException {
		// TODO Auto-generated method stub
		// down Project.DaemonNum fois
		for (int i=0; i<DaemonNum;i++)
			try {
				Mappers.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	@Override
	public void mapperterminer() throws RemoteException {
		// TODO Auto-generated method stub
		// chaque mapper va faire une up 
		Mappers.release();
	}

}
