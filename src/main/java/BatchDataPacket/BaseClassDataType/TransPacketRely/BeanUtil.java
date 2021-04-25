package BatchDataPacket.BaseClassDataType.TransPacketRely;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import ty.pub.RawDataPacket;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BeanUtil {

	static Kryo kryo = new Kryo();

	/**
	 * 对象转数组
	 * 
	 * @param obj
	 * @return
	 */
	public synchronized static byte[] toByteArray(Object obj) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output output = new Output(bos, 40960);
		kryo.writeObject(output, obj);
		byte[] bytes = output.toBytes();
		output.close();
		return bytes;
	}

	/**
	 * 数组转对象
	 * 
	 * @param bytes
	 * @return
	 */
	public synchronized static <T> T toObject(byte[] bytes, Class<T> type) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		Input input = new Input(bis);
		T obj = kryo.readObject(input, type);
		input.close();
		return obj;
	}

	/**
	 * byte[] 转 十六进制字符串，长度*2
	 * 
	 * @param bArray
	 * @return
	 */
	public static String bytesToHexString(byte[] bArray) {
		StringBuffer sb = new StringBuffer(bArray.length);
		String sTemp;
		for (int i = 0; i < bArray.length; i++) {
			sTemp = Integer.toHexString(0xFF & bArray[i]);
			if (sTemp.length() < 2)
				sb.append(0);
			sb.append(sTemp.toUpperCase());
		}
		return sb.toString();
	}

	/**
	 * 十六进制字符串转字符数组，长度/2
	 * 
	 * @param hex
	 * @return
	 */
	public static byte[] hexStringToByte(String hex) {
		int len = (hex.length() / 2);
		byte[] result = new byte[len];
		char[] achar = hex.toCharArray();
		for (int i = 0; i < len; i++) {
			int pos = i * 2;
			result[i] = (byte) (toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
		}
		return result;
	}

	/**
	 * long to byte[]
	 * 
	 * @param values
	 * @return
	 */
	public static byte[] LongToBytes(long values) {
		byte[] buffer = new byte[8];
		for (int i = 0; i < 8; i++) {
			int offset = 64 - (i + 1) * 8;
			buffer[i] = (byte) ((values >> offset) & 0xff);
		}
		return buffer;
	}

	/**
	 * Byte[] to long
	 * 
	 * @param buffer
	 * @return
	 */
	public static long BytesToLong(byte[] buffer) {
		long values = 0;
		for (int i = 0; i < 8; i++) {
			values <<= 8;
			values |= ((byte) buffer[buffer.length - i - 1] & 0xff);
		}
		return values;
	}

	/**
	 * char to Byte
	 * 
	 * @param c
	 * @return
	 */
	private static byte toByte(char c) {
		byte b = (byte) "0123456789ABCDEF".indexOf(c);
		return b;
	}

	public static RawDataPacket split(byte[] bytes) {
		int length = bytes.length;
		List<Integer> splits = new ArrayList<Integer>();

		splits.add(-2);
		for (int j = 0; j < (length - 1)  && (splits.size() <8); j++) {
			if (bytes[j] == 13 && bytes[j + 1] == 10) {
				splits.add(j);
			}
		}
		splits.add(length - 8);
		List<byte[]> containers = new ArrayList<byte[]>();
		for (int i = 0; i < splits.size() - 1; i++) {
			containers.add(Arrays.copyOfRange(bytes, splits.get(i) + 2, splits.get(i + 1)));
		}
		long timestamp = BytesToLong(Arrays.copyOfRange(bytes, length - 8, length));
		try {
			return new RawDataPacket(new String(containers.get(0), "UTF-8"), new String(containers.get(1), "UTF-8"),
					new String(containers.get(2), "UTF-8"), new String(containers.get(3), "UTF-8"),
					new String(containers.get(4), "UTF-8"), new String(containers.get(5), "UTF-8"),
					new String(containers.get(6), "UTF-8"), containers.get(7), timestamp);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static String ObjectToString(Object obj) {
		StringBuffer result = new StringBuffer();
		Class cls = obj.getClass();
		result.append(cls.getName() + ":{");
		Field[] fields = cls.getDeclaredFields();
		for (int i = 0; i < fields.length; i++) {
			Field f = fields[i];
			f.setAccessible(true);
			try {
				String out = String.format("%s : %s", f.getName(), f.get(obj));
				if (i < fields.length - 1)
					out += ", ";
				result.append(out);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		result.append("}");
		return result.toString();
	}

}
