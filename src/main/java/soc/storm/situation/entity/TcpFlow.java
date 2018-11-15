
package soc.storm.situation.entity;

import java.util.Arrays;
import java.util.Random;
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


		String separator1 = "metadata - - -";
		String separator2 = "notice - - -";
		String testGJ = "Chr <14>1 2017-05-08T08:29:18.334Z 192.168.10.212 notice - - - 1494232158334^ATD^10.0.0.172^NDE^7c938b87-c0f7-4689-938b-87c0f7d689c9^p6p1^2016149^........................^INFO^Session Traversal^192.168.20.187^64110^^54.172.47.69^3478^^UDP^^............^....!..BXMaESP7SRWtW^^^^^SDE INFO Session Traversal Utilities for NAT (STUN Binding Request)^^SDE INFO Session Traversal Utilities for NAT (STUN Binding Request)^China^Beijing^39.9047,116.4072^CN^United States^Ashburn^39.0481,-77.4728^US^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^";
		String testLL = "Chr <14>1 2018-10-25T23:31:03.341Z 10.0.0.168 metadata - - - 1540510263340^ATD^10.0.0.249^bde_dns^eth1^1540510210.811544^CQnDD40J1qtAwSEG5^10.0.4.22^62983^224.0.0.252^5355^udp^18475^^desktop-2qorao4^1^C_INTERNET^255^*^^^F^F^F^F^0^^^F^^^^^^^^^^^^^^^^^^^^^^";

//		String input = testGJ;
		String input = testLL;
		String log = "";
		String[] logsLL = input.split(separator1);
		String[] logsGJ = input.split(separator2);
		if(logsLL.length == 2){
			System.out.println("LL log is: " + logsLL[1]);
		}else if(logsGJ.length == 2){
			System.out.println("GJ log is: " + logsGJ[1]);
		}
		int j = 0;
		while (j < 10){
			System.out.println(new Random().nextInt(9));
			j++;
		}

	}

}
