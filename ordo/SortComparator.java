/* une PROPOSITION, incomplète et adaptable... */

package ordo;
public interface SortComparator {
	public int compare(String k1,String k2);
	public void sortKvfile(String filename);
	public void compareKvfile(String file1,String file2);
}
