package cn.situation.data;

import java.io.File;
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
import org.slf4j.LoggerFactory;

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
		typeMap = new HashMap<>();
		typeMap.put("0x01", "23000");
		typeMap.put("0x02", "27000");
		typeMap.put("0x03", "27000");
		typeMap.put("0x04", "28000");
		typeMap.put("0x05", "29000");
		typeMap.put(null, "29000");
	}
	
	/**
	 * 资产字段每行正常情况下为43个字段
	 * @param lines
	 */
	public static void do_trans(List<String> lines) {
		List<String[]> assetList = new ArrayList<>();
		for (String line : lines) {
			try {
				if(StringUtils.isNotBlank(line)) {
					boolean null_flag = false;
					//防止出现数据为"222||"类似结构情况下,字符串猜分将末尾空字符串舍弃的情况
					if(line.endsWith("|")) {
						line += ";";
						null_flag = true;
					}
					String[] assetArray = line.split("\\|");
					if(assetArray.length != ELEMENT_NUM) {
						logger.error(String.format("msg：【[%s]】字段个数不对", line));
						return;
					}
					if(!ASSET_TYPE.equals(assetArray[2])) {
						logger.error(String.format("msg：【[%s]】类型非内网资产发现消息", line));
						return;
					}
					//将末尾字符串替换
					if (null_flag) {
						assetArray[assetArray.length - 1] = "";
					}
					assetList.add(assetArray);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		try {
			List<String[]> updateAssetList = new ArrayList<>();
			List<String[]> insertAssetList = new ArrayList<>();
			//按照设备IP将资产信息去重
			for(String[] arrTemp : assetList) {
				if(StringUtils.isNotBlank(arrTemp[41]) && isExist(arrTemp[41])) {
					updateAssetList.add(arrTemp);
				}else if(StringUtils.isNotBlank(arrTemp[41])) {
					insertAssetList.add(arrTemp);
				}
			}

			//入库资产数据
			if(insertAssetList.size() > 0) saveAssets(insertAssetList);
			//更新资产
			if(updateAssetList.size() > 0) updateAssets(updateAssetList);
			logger.info(String.format("[%s]: insertAssetSize<%s>, updateAssetSize<%s>", "do_trans",
					insertAssetList.size(), updateAssetList.size()));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}
	
	/**
	 * 根据IP判断单位资产是否存在
	 * @return
	 */
	public static boolean isExist(String ip) {
		boolean flag = false;
		PgUtil pu = new PgUtil();
		PreparedStatement pre = null;
		String sql = "SELECT COUNT(1) FROM T_THIRD_ASSET_HARDWARE WHERE \"ip\" = ?";
		try {
			pre = pu.getPreparedStatement(sql);
			pre.setString(1, ip);
			
			ResultSet res = pre.executeQuery();
			while(res.next()) {
				if(res.getInt(1) > 0) {
					flag = true;
				}
			}
			res.close();
		} catch (Exception e) {
			logger.error("资产重复判断失败!", e.getMessage());
		}finally {
			pu.destory();
		}
		return flag;
	}

	private static void updateAssets(List<String[]> assetList) {
		PgUtil pu = new PgUtil();
		PreparedStatement pre = null;
		try {
			String sql = "UPDATE T_THIRD_ASSET_HARDWARE set \"res_name\"=?,\"res_type\"=?,\"res_code\"=?,"
					+ "\"source_manufacturers_type\"=?,\"res_model\"=?,\"make_manufacturers_name\"=?,\"phy_position\"=?,\"os_name\"=?,\"os_version\"=?,"
					+ "\"source_manufacturers_info\"=?,\"is_virtual\"=?,\"ip\"=?,\"apps\"=?,\"extend_info\"=? WHERE \"ip\" = ?";
			pre = pu.getPreparedStatement(sql);
			
			int batch_num_temp = 1;
			Map<String, Object> infoMap = null;
			Map<String, Object> appMap = null;
			for(String[] assetArray : assetList) {
				int index = 1;
				pre.setString(index++, assetArray[19]);
				pre.setString(index++, coverResType(assetArray[18]));
				pre.setString(index++, assetArray[17]);
				pre.setString(index++, assetArray[20]);
				pre.setString(index++, assetArray[21]);
				pre.setString(index++, assetArray[23]);
				pre.setString(index++, assetArray[24]);
				pre.setString(index++, assetArray[25]);
				pre.setString(index++, assetArray[26]);
				if(StringUtils.isBlank(assetArray[27])) {
					pre.setNull(index++, Types.NULL);
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
					pre.setNull(index++, Types.NULL);
				}
				pre.setObject(index++, coverToJson(assetArray));
				pre.setObject(index++, assetArray[41]);
				pre.addBatch();
				
				if(batch_num_temp++ >= BATCH_MAX_NUM) {
					pre.executeBatch();
					batch_num_temp = 1;
				}
			}
			if(batch_num_temp > 1) pre.executeBatch();
			logger.info(String.format("[%s]: updateAssets<%s>", "saveAssets", batch_num_temp));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}finally {
			pu.destory();
		}
	}
	
	/**
	 * 新增资产信息
	 * @param assetList
	 */
	private static void saveAssets(List<String[]> assetList) {
		PgUtil pu = new PgUtil();
		PreparedStatement pre = null;
		try {
			String sql = "INSERT INTO T_THIRD_ASSET_HARDWARE(\"res_name\",\"res_type\",\"res_code\","
					+ "\"source_manufacturers_type\",\"res_model\",\"make_manufacturers_name\",\"phy_position\",\"os_name\",\"os_version\","
					+ "\"source_manufacturers_info\",\"is_virtual\",\"ip\",\"apps\",\"extend_info\", \"status\", \"flow_status\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)";
			pre = pu.getPreparedStatement(sql);
			
			int batch_num_temp = 1;
			Map<String, Object> infoMap = null;
			Map<String, Object> appMap = null;
			for(String[] assetArray : assetList) {
				int index = 1;
				pre.setString(index++, assetArray[19]);
				pre.setString(index++, coverResType(assetArray[18]));
				pre.setString(index++, assetArray[17]);
				pre.setString(index++, assetArray[20]);
				pre.setString(index++, assetArray[21]);
				pre.setString(index++, assetArray[23]);
				pre.setString(index++, assetArray[24]);
				pre.setString(index++, assetArray[25]);
				pre.setString(index++, assetArray[26]);
				if(StringUtils.isBlank(assetArray[27])) {
					pre.setNull(index++, Types.NULL);
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
					pre.setNull(index++, Types.NULL);
				}
				pre.setObject(index++, coverToJson(assetArray));
				pre.addBatch();
				
				if(batch_num_temp++ >= BATCH_MAX_NUM) {
					pre.executeBatch();
					batch_num_temp = 1;
				}
			}
			if(batch_num_temp > 1) pre.executeBatch();
			logger.info(String.format("[%s]: batch_num_temp<%s>", "saveAssets", batch_num_temp));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}finally {
			pu.destory();
		}
	}
	
	/**
	 * 将数组中其他字段转换为json字符串
	 * @param assetArray： 资产发现数组
	 * @return
	 */
	private static PGobject coverToJson(String[] assetArray) throws Exception {
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
	
	/**
	 * 测试方法, 将解析的数组生成json串打印
	 * @param strArray: 解析后的字符串数组
	 */
	private static String arrayToJon(String[] strArray) {
		Map<String, Object> strMap = new HashMap<String, Object>();
		strMap.put("domain", strArray[17]);
		strMap.put("asset_device_type", strArray[18]);
		strMap.put("asset_device_id", strArray[19]);
		strMap.put("asset_device_model", strArray[20]);
		strMap.put("serial_number", strArray[21]);
		strMap.put("Imei", strArray[22]);
		strMap.put("net_device_vendor", strArray[23]);
		strMap.put("location", strArray[24]);
		strMap.put("os", strArray[25]);
		strMap.put("os_version", strArray[26]);
		strMap.put("os_vender", strArray[27]);
		strMap.put("software", strArray[28]);
		strMap.put("software_version", strArray[29]);
		strMap.put("middleware", strArray[30]);
		strMap.put("plug-in", strArray[31]);
		strMap.put("container", strArray[32]);
		strMap.put("container_version", strArray[33]);
		strMap.put("framework", strArray[34]);
		strMap.put("framework_version", strArray[35]);
		strMap.put("component", strArray[36]);
		strMap.put("component_version", strArray[37]);
		strMap.put("agent", strArray[38]);
		strMap.put("vpn", strArray[39]);
		strMap.put("imulator", strArray[40]);
		strMap.put("device_ip", strArray[41]);
		strMap.put("device_mac", strArray[42]);
		
		return JsonUtil.mapToJson(strMap);
	}
	
    public static void main(String[] args) throws Exception {
    	File[] files = new File("C:\\Users\\quanli\\Desktop\\asset").listFiles();
    	for(File file : files) {
    		List<String> assetsList = FileUtil.getFileContentByLine(file.getAbsolutePath(), false);
            logger.info(String.format("[%s]: assetsList<%s>", "handleDevAssets", assetsList));
            if(assetsList != null) {
            }
    	}
	}
}
