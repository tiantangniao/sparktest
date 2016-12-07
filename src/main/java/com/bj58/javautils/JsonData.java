package com.bj58.javautils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.List;

/**
 * Created by 58 on 2016/11/16.
 */
public class JsonData {
	public static String[] adSerchStringToJSONArray(String str) {
		JSONArray jsonArray = JSONArray.fromObject(str);
		String[] result = new String[jsonArray.size()];
		StringBuilder ietmstr = new StringBuilder();
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject ietm = jsonArray.getJSONObject(i);
			ietmstr.append(ietm.getString("adtype")).append("\001").append(ietm.getString("aduserid")).append("\001")
					.append(ietm.getString("subid")).append("\001").append(ietm.getString("infoid")).append("\001")
					.append(ietm.getString("bid")).append("\001").append(ietm.getString("price")).append("\001").append(ietm.getString("q"))
					.append("\001").append(ietm.getString("pos")).append("\001").append(ietm.getString("slot"));
			result[i] = ietmstr.toString();
			ietmstr.delete(0, ietmstr.length());
		}
		return result;
	}

	public static int getjsonnum(String strlist) {
		int num = 0;
		try {
			JSONArray jsonArray = JSONArray.fromObject(strlist);
			num = jsonArray.size();
		} catch (Exception e) {

		}

		return num;
	}

	public static String adGroupBysidToOne(List<String> strlist) {
		String reslut = "-";
		if (null != strlist) {
			if (strlist.size() == 1) {
				String stritem = strlist.get(0);
				if (stritem.split("\001").length == 34) {
					stritem = stritem + "\001" + "-" + "\001" + "-" + "\001" + "-" + "\001" + "-";
					reslut = stritem;
				}
			} else if (strlist.size() == 2) {
				String stritem1 = strlist.get(0);
				String stritem2 = strlist.get(1);
				String[] stritem1words = stritem1.split("\001");
				String[] stritem2words = stritem2.split("\001");
				if (stritem1words.length == 34 && stritem2words.length == 24) {
					reslut = stritem1 + "\001" + stritem2words[10] + "\001" + stritem2words[11] + "\001" + stritem2words[22] + "\001"
							+ stritem2words[23];
				} else if (stritem1words.length == 24 && stritem2words.length == 34) {
					reslut = stritem2 + "\001" + stritem1words[10] + "\001" + stritem1words[11] + "\001" + stritem1words[22] + "\001"
							+ stritem1words[23];
				}
			}
		}
		return reslut;
	}

	public static void main(String args[]) {
		for (int i = 0; i < 1000; i++) {
			System.out.println("#" + i + "w" + "%" + (i - 1));
		}
	}
}
