
package soc.storm.situation.coder;

import java.util.List;

/**
 * 
 * @author wangbin03
 *
 */
public interface WebFlowLogGatherMsgCoder {
    // byte[] toWire(WebFlowLogGatherMsg webFlowLogGatherMsg) ;
    //
    // WebFlowLogGatherMsg fromWire(byte[] input) ;

    List<Object> fromWire(byte[] input);
}
