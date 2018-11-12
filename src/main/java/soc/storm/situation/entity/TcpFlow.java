
package soc.storm.situation.entity;

import java.util.Arrays;
import java.util.StringTokenizer;

@Deprecated
public class TcpFlow {
	
	public static void main(String[] args) {
		String ss = "1540513948934^ATD^10.0.0.249^bde_mysql^pcap^1538991593.118519^C2uTGu4p6ALVdVQ3Y7^192.168.2.224^36703^10.0.0.173^3306^login^^^1^^^^^^^^^^^^^^^^^^^^^8;q=0.7,*;q=0.7KEEP-ALIVE:300CONNECTION:keep-aliveREFERER:http://www.ethereal.com/development.html^^^/download.html^^^^^^^^^^^^^^^^^^^^^^";
		String testString = "1540510214312^ATD^10.0.0.249^bde_conn^eth1^1540510093.198195^C0vT5L2OAUaMv8O0Dj^fe80::ae1f:6bff:fe6e:2e07^546^ff02::1:2^547^udp^^^^^S0^F^F^0^D^1^76^0^0^^^^^other^^^^^ac:1f:6b:6e:2e:07^33:33:00:01:00:02^^end^^^^^^^^^^^^^^^^^^^^";
		String[] sslist = testString.split("\\^", -1);
		System.out.println("tcp / udp : " + sslist[11]);
		System.out.println(sslist.length);
		System.out.println(Arrays.toString(sslist));
		System.out.println(sslist[sslist.length-1]);
		
		StringTokenizer ssslist = new StringTokenizer(ss, "^");
		int i = 0;
		while(ssslist.hasMoreTokens()){
		    System.out.println(ssslist.nextToken());
		    i++;
		}
		System.out.println(i);
	}

}
