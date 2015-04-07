package com.elex.hive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class AresMerge {
	public static String urlStr = "jdbc:hive2://namenode:10000/default";
	public static String driverStr = "org.apache.hive.jdbc.HiveDriver";
	public static String dataUser = "hadoop";
	public static String dataPass = "";

	public static Connection getCon() {
		Connection con = null;
		try {
			Class.forName(driverStr);
			con = DriverManager.getConnection(urlStr, dataUser, dataPass);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return con;
	}

	public static boolean mergeData(String sql) {
		Connection con = getCon();
		boolean mark = false;
		try {
			Statement stmt = con.createStatement();
			mark = stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mark;
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		 Properties prop = new Properties();
		 prop.load(AresMerge.class.getClassLoader().getResourceAsStream("conf.properties"));
		 String startDay = prop.getProperty("startday");
		 String endDay = prop.getProperty("endday");
		String sql = "insert overwrite table ares_merge1 select t.campid, t.nation, t.size, t.language, t.site, t.slot, t.browser, t.time, sum(c.confirmed_sum), sum(c.confirmed_dollar), sum(c.other_sum), sum(c.other_dollar), max(c.max_delay), count(t.reqid),count(c.clickid) "
				+ "from (select uid as uid, reqid as reqid,nation as nation,size as size,language as language,site as site,slot as slot,browser as browser,timeframe(nation,time) as time,camp as campid,ad as adid "
				+ "from ares_impression lateral view splitcampidudtf(concat(camp_id,':',adid)) udtf as camp, ad where (day >=" + startDay + "and day <= " + endDay + ")) "
				+ "t left join (select uid as uid, reqid as reqid, count(clickid) as clickid, campid as campid, sum(confirmed_sum) as confirmed_sum, sum(confirmed_dollar) as confirmed_dollar, sum(other_sum) as other_sum, sum(other_dollar) as other_dollar,max(max_delay) as max_delay "
				+ "from ares_click_trans where (campid is not null and reqid is not null and clickid is not null) group by reqid, campid, uid) "
				+ "c on (t.uid = c.uid and t.reqid = c.reqid and t.campid = c.campid) group by t.nation, t.size, t.language, t.site, t.slot, t.browser, t.time, t.campid";
		
		mergeData(sql);
	}
}
