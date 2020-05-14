

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
import hdfs.HdfsClientHelper;
import hdfs.NameNode;

import config.Project;

public class WriteFile {




public static void main ( String args[] ){


						      HdfsClientHelper.hdfswrite(Project.PATH+"/"+args[0],"LINE",Project.facteurdedubplication,Project.facteurfragmentation); 


}



}
