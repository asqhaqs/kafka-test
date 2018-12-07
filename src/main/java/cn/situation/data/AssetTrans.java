package cn.situation.data;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.situation.util.LogUtil;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;

import cn.situation.util.FileUtil;
import cn.situation.util.JsonUtil;
import cn.situation.util.PgUtil;

/**
 * 流量资产数据解析入库操作
 * @author quanli
 *
 */
public class AssetTrans {
	
	private final static Logger logger = LogUtil.getInstance(AssetTrans.class);
	//资产行数据字段个数
	private final static int ELEMENT_NUM = 43;
	//批量插入数据最大个数
	private final static int BATCH_MAX_NUM = 1000;
	//内网资产发现消息消息类型
	private final static String ASSET_TYPE = "0x0400";
	private static Map<String, String> typeMap = null;
	
	static {
		if(typeMap == null) {
			typeMap = new HashMap<String, String>();
			
			typeMap.put("0x01", "23000");
			typeMap.put("0x02", "27000");
			typeMap.put("0x03", "27000");
			typeMap.put("0x04", "28000");
			typeMap.put("0x05", "29000");
			typeMap.put(null, "29000");
		}
	}
	
	/**
	 * 资产字段每行正常情况下为43个字段
	 * @param assetsList
	 */
	public static void do_trans(List<String> assetStrList) {
		List<String[]> assetList = new ArrayList<String[]>();
		for(String assetStr : assetStrList) {
			if(StringUtils.isNotBlank(assetStr)) {
				String[] assetArray = assetStr.split("\\|");
				if(assetArray.length != ELEMENT_NUM) {
					logger.error(String.format("msg：【[%s]】字段个数不对", assetStr));
					continue;
				}
				
				if(!ASSET_TYPE.equals(assetArray[2])) {
					logger.error(String.format("msg：【[%s]】类型非内网资产发现消息", assetStr));
					continue;
				}
				assetList.add(assetArray);
			}
		}
		
		//入库资产数据
		saveAssets(assetList);
	}

	/**
	 * 存储资产信息
	 * @param assetList
	 */
	private static void saveAssets(List<String[]> assetList) {
		PgUtil pu = PgUtil.getInstance();
		Connection conn = null;
		PreparedStatement pre = null;
		ResultSet res = null;
		try {
			conn = pu.getConnection();
			pre = conn.prepareStatement("INSERT INTO T_ASSETS_DEV(\"res_name\",\"res_type\",\"res_code\","
					+ "\"source_type\",\"res_model\",\"manufactures_name\",\"phy_position\",\"os_name\",\"os_version\","
					+ "\"source_info\",\"is_virtual\",\"ip\",\"apps\",\"extend_info\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			int batch_num_temp = 1;
			Map<String, Object> infoMap = null;
			Map<String, Object> appMap = null;
			for(String[] assetArray : assetList) {
				int index = 1;
				pre.setString(index++, assetArray[17]);
				pre.setString(index++, coverResType(assetArray[18]));
				pre.setString(index++, assetArray[19]);
				pre.setString(index++, assetArray[20]);
				pre.setString(index++, assetArray[21]);
				pre.setString(index++, assetArray[23]);
				pre.setString(index++, assetArray[24]);
				pre.setString(index++, assetArray[25]);
				pre.setString(index++, assetArray[26]);
				if(StringUtils.isBlank(assetArray[26])) {
					pre.setNull(index++, Types.JAVA_OBJECT);
				}else {
					infoMap = new HashMap<String, Object>();
					infoMap.put("os_vender", assetArray[26]);
					PGobject jsonObject = new PGobject();
					jsonObject.setType("json");
					jsonObject.setValue(JsonUtil.mapToJson(infoMap));
					pre.setObject(index++, jsonObject);
				}
				if(StringUtils.isBlank(assetArray[40])) {
					pre.setNull(index++, Types.INTEGER);
				}else {
					pre.setInt(index++, Integer.parseInt(assetArray[40]));
				}
				pre.setString(index++, assetArray[41]);
				if(StringUtils.isNotBlank(assetArray[28]) || StringUtils.isNotBlank(assetArray[29])) {
					appMap = new HashMap<String, Object>();
					appMap.put("name", assetArray[28]);
					appMap.put("version", assetArray[29]);
					
					PGobject jsonObject = new PGobject();
					jsonObject.setType("json");
					jsonObject.setValue(JsonUtil.mapToJson(appMap));
					pre.setObject(index++, jsonObject);
				}else {
					pre.setNull(index++, Types.JAVA_OBJECT);
				}
				pre.setObject(index++, coverToJson(assetArray));
				pre.addBatch();
				
				if(batch_num_temp++ >= BATCH_MAX_NUM) {
					pre.executeBatch();
					batch_num_temp = 1;
				}
			}
			if(batch_num_temp > 1) pre.executeBatch();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}finally {
			pu.destory(conn, pre, res);
		}
	}
	
	/**
	 * 将数组中其他字段转换为json字符串
	 * @param assetArray： 资产发现数组
	 * @return
	 */
	private static PGobject coverToJson(String[] assetArray) throws Exception{
		PGobject jsonObject = new PGobject();
		Map<String, Object> assetMap = new HashMap<String, Object>();
		assetMap.put("imei", assetArray[22]);
		assetMap.put("middleware", assetArray[30]);
		assetMap.put("plug-in", assetArray[31]);
		assetMap.put("container", assetArray[32]);
		assetMap.put("container_version", assetArray[33]);
		assetMap.put("framework", assetArray[34]);
		assetMap.put("framework_version", assetArray[35]);
		assetMap.put("component", assetArray[36]);
		assetMap.put("component_version", assetArray[37]);
		assetMap.put("agent", assetArray[38]);
		assetMap.put("vpn", assetArray[39]);
		assetMap.put("device_mac", assetArray[42]);
		
		String jsonStr = JsonUtil.mapToJson(assetMap);
		jsonObject.setType("json");
		jsonObject.setValue(jsonStr);
		return jsonObject;
	}
	
	/**
	 * 转换资产发现中的资产类型为系统资产类型
	 * @param foundType：资产发现类型
	 * @return
	 */
	private static String coverResType(String foundType) {
		String assetType = typeMap.get(foundType);
		if(StringUtils.isBlank(assetType)) {
			assetType = "29000";
		}
		return assetType;
	}
	
    public void test() {
    	File[] files = new File("C:\\Users\\quanli\\Desktop\\asset").listFiles();
    	for(File file : files) {
    		List<String> assetsList = FileUtil.getFileContentByLine(file.getAbsolutePath(), true);
            logger.info(String.format("[%s]: assetsList<%s>", "handleDevAssets", assetsList));
            if(assetsList != null) {
            	AssetTrans.do_trans(assetsList);
            }
    	}
    }

}
