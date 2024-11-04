package zipkin2.clickhouse.util;

import com.google.gson.Gson;

import java.lang.reflect.Type;


public class JSONUtils {

	private static final Gson GSON = new Gson();

	/**
	 * 对象转JSON字符串
	 *
	 * @param obj
	 *            对象
	 * @return
	 */
	public static String toJSONString(Object obj) {
		if (obj == null) {
			return null;
		}
		return GSON.toJson(obj);
	}
	/**
	 * JSON字符串转POJO对象
	 *
	 * @param data
	 *            JSON字符串
	 * @param clazz
	 *            POJO类
	 * @param <T>
	 * @return
	 */
	public static <T> T parseObject(String data, Class<T> clazz) {
		if (data == null || clazz == null) {
			return null;
		}
		return GSON.fromJson(data, clazz);
	}

	/**
	 * JSON字符串转POJO对象
	 *
	 * @param data
	 *            JSON字符串
	 * @param type
	 *            POJO类
	 * @param <T>
	 * @return
	 */
	public static <T> T parseObject(String data, Type type) {
		if (data == null || type == null) {
			return null;
		}
		return GSON.fromJson(data, type);
	}

}
