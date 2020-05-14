package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import map.Mapper;
import map.Reducer;
import formats.Format;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;
	
	
	public void runReduce(Reducer m, HashMap<String, ArrayList<Format>> reducemap, /*HashMap<String, Format>*/Format outputreducerformat, CallBack cb) throws RemoteException;

	public void runReduce1(Reducer m,Format reducefile1,Format reducefile2,Format reduceres, CallBack cbr) throws RemoteException;
}
