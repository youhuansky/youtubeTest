package com.youtube.utl;

public class ETLUtils {

	/**
	 * @Description： 1.过滤不合法数据 2.去掉&符号之间的空格 3.\t换成&符号
	 * <p>
	 * 创建人：Administrator , 2018年7月20日 下午2:52:09
	 * </p>
	 * <p>
	 * 修改人：Administrator , 2018年7月20日 下午2:52:09
	 * </p>
	 *
	 * @param str
	 * @return String
	 */
	public static String getETLString(String str) {

		String[] split = str.split("\t");
		if (split.length < 9) {
			System.out.println("长度小于9被过滤");
			return null;
		}
		split[3] = split[3].replaceAll(" ", "");

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < split.length; i++) {
			if (i >= 9) {
				sb.append(split[i]);
				if (i != split.length - 1) {
					sb.append("&");
				}
			} else {
				sb.append(split[i]);
				if (i != split.length - 1) {
					sb.append("\t");
				}
			}
		}

		return sb.toString();
	}
}
