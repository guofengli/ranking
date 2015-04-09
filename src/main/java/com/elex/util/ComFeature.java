package com.elex.util;

import java.util.ArrayList;

public class ComFeature {
	public static ArrayList<String> getFeatures(String []features) {
		String[] prefixs = { "nation_", "size_", "language_", "site_", "slot_", "browser_", "time_" };
		ArrayList<String> featureList = new ArrayList<String>();
		int[] num = new int[8];
		num[0] = 1;
		for (int i = 1; i < num.length; i++) {
			num[i] = 0;
		}
		while (num[7] == 0) {
			StringBuffer strTMP = new StringBuffer();
			for (int i = 0; i < 7; i++) {
				if (num[i] == 1)
					strTMP.append(prefixs[i] + features[i] + ":");
			}
			int mark = 1, i = 0;
			while (mark == 1) {
				num[i] += mark;
				if (num[i] > 1) {
					num[i++] = 0;
					mark = 1;
				} else
					mark = 0;
			}
			featureList.add(strTMP.toString());
		}
		return featureList;
	}
}
