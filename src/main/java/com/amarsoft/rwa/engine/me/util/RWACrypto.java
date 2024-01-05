package com.amarsoft.rwa.engine.me.util;

/**
 * RWA加密解密工具类
 * @author 陈庆
 * @version 1.0 2013-10-31
 *
 */
public class RWACrypto {

	/**
	 * 将字符串加密
	 * @param code 字符串
	 * @return 加密后的字符串
	 */
	public static synchronized String encode(String code) {
		return Base64.getEncodeString(code);
	}
	
	/**
	 * 将加密的字符串解密
	 * @param encoded 加密后的字符串
	 * @return 解密后的原字符串
	 */
	public static synchronized String decode(String encoded) {
		return Base64.getDecodeString(encoded);
	}
	
}
