package ordo;

import java.io.Serializable;


public class DataDaemoninfo implements Serializable {

	public static String firstInputfile;
	private String ip;
	private int port;
	private String rmiadresse;
	public DataDaemoninfo(String ip, int port, String rmiadresse) {
		super();
		this.ip = ip;
		this.port = port;
		this.rmiadresse = rmiadresse;
	}

	public static String getFirstInputfile() {
		return firstInputfile;
	}

	public static void setFirstInputfile(String firstInputfile) {
		DataDaemoninfo.firstInputfile = firstInputfile;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getRmiadresse() {
		return rmiadresse;
	}

	public void setRmiadresse(String rmiadresse) {
		this.rmiadresse = rmiadresse;
	}

	public String getDatatempfile() {
		return datatempfile;
	}

	public void setDatatempfile(String datatempfile) {
		this.datatempfile = datatempfile;
	}

	public void setMappertempfile(String mappertempfile) {
		this.mappertempfile = mappertempfile;
	}

	private String mappertempfile;
	private String datatempfile;
	
	
	public String getMappertempfile() {
		// TODO Auto-generated method stub
		return mappertempfile;
	}

	public static String getfirstInputfile() {
		// TODO Auto-generated method stub
		return firstInputfile;
	}

}
