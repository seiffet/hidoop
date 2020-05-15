package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import map.MapReduce;
import ordo.Job;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import java.lang.Math;

public class MontCarlo implements MapReduce {
	private static final long serialVersionUID = 1L;


	public void map(FormatReader reader, FormatWriter writer) {
		KV kv;
		long inside=0L;
		long outside =0L;
		double x= 0.3;
		double y= 0.5;
		while ((kv = reader.read()) != null) {
			// in order to reduce correlation  we use a value from the initial file and a random number from java lib
			if ((kv.k).equals("x")) {x=Double.parseDouble(kv.v);y=Math.random();}
			else {y=Double.parseDouble(kv.v);x=Math.random();}
			x*=x;
			y*=y;
			double distance = Math.sqrt(x+y);
			if(distance <= 1) inside ++;
			else outside ++; 
		}
		writer.write(new KV("IN",inside+""));
		writer.write(new KV("OUT",outside+""));
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	public static void main(String args[]) {
		
		// le nom de fichier dans les argume
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        
       long t1 = System.currentTimeMillis();
		j.startJob(new MontCarlo());
		long t2 = System.currentTimeMillis();
		//get the result here and calculte PI
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
