package com.bj58.javautils;

import com.clearspring.analytics.util.Lists;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
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
            ietmstr.append(ietm.getString("adtype")).append("\001")
                    .append(ietm.getString("aduserid")).append("\001")
                    .append(ietm.getString("subid")).append("\001")
                    .append(ietm.getString("infoid")).append("\001")
                    .append(ietm.getString("bid")).append("\001")
                    .append(ietm.getString("price")).append("\001")
                    .append(ietm.getString("q")).append("\001")
                    .append(ietm.getString("pos")).append("\001")
                    .append(ietm.getString("slot"));
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

    public static String adGroupBysidToOne(List<String> strList) {
        String result = "-";
        if (null != strList) {
            if (strList.size() == 1) {
                StringBuffer stritem = new StringBuffer(strList.get(0));
                if (stritem.toString().split("\001").length == 34) {
                    stritem = stritem.append("\001").append("-").append("\001").append("-").append("\001").append("-").append("\001").append("-");
                    result = stritem.toString();
                }
            } else if (strList.size() > 1) {
                int i;
                for (i = 0; i < strList.size(); i++) {
                    if (strList.get(i).split("\001").length == 34) {
                        break;
                    }
                }
                if (i >= strList.size()) {
                    return result;
                }
                StringBuffer strDes = new StringBuffer(strList.get(i));
                String strOur = "";
                String strOther = "";
                for (String strItem : strList) {
                    if (strItem.split("\001").length == 24) {
                        strOur = strItem;
                    }
                    if (strItem.split("\001").length == 53) {
                        strOther = strItem;
                    }
                }

                if (!("").equals(strOur)) {
                    String[] ourStrWords = strOur.split("\001");
                    strDes = strDes.append("\001").append(ourStrWords[10]).append("\001").append(ourStrWords[11]);
                } else {
                    strDes = strDes.append("\001").append("-").append("\001").append("-");
                }
                if (!("").equals(strOther)) {
                    String[] otherStrWords = strOther.split("\001");
                    strDes = strDes.append("\001").append(otherStrWords[28]).append("\001").append(otherStrWords[51]);
                } else {
                    strDes = strDes.append("\001").append("-").append("\001").append("-");
                }
                result = strDes.toString();
            }
        }
        return result;
    }

    public static void main(String args[]) {
        for (int i = 0; i < 1000; i++) {
            System.out.println("#" + i + "w" + "%" + (i - 1));
        }
    }
}
