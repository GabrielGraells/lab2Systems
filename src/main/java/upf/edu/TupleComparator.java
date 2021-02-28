package upf.edu;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;
public class TupleComparator implements Comparator<Tuple2<Integer,Long>>,
        Serializable {
	@Override
	public int compare(Tuple2<Integer,Long> tuple1,
	        Tuple2<Integer, Long> tuple2) {
	    return tuple1._1 - tuple2._1;
	}
}