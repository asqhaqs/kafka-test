
package soc.storm.situation.entity;

import java.util.Arrays;
import java.util.StringTokenizer;

@Deprecated
public class TcpFlow {
	
	public static void main(String[] args) {
		String ss = "1540513948934^ATD^10.0.0.249^bde_mysql^pcap^1538991593.118519^C2uTGu4p6ALVdVQ3Y7^192.168.2.224^36703^10.0.0.173^3306^login^^^1^^^^^^^^^^^^^^^^^^^^^8;q=0.7,*;q=0.7KEEP-ALIVE:300CONNECTION:keep-aliveREFERER:http://www.ethereal.com/development.html^^^/download.html^^^^^^^^^^^^^^^^^^^^^^";
		String[] sslist = ss.split("\\^", -1);
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
