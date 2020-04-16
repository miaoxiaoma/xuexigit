package com.example.demo.conusmer;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
@Slf4j
public class RabbitReceiver {

	private static final String test="你好";
	private static final String E_QUE="E.SS.QUE";
	@RabbitListener(bindings = @QueueBinding(
			value = @Queue(value = E_QUE,
			durable="true"),
			exchange = @Exchange(value =E_QUE,
			durable="true", 
			type= "topic", 
			ignoreDeclarationExceptions = "true"),
			key = "springboot.*"
			)
	)
	@RabbitHandler
	public void onMessage(Message message, Channel channel) throws Exception {
		System.err.println("--------------------------------------");
		//System.out.println(message);
		//System.err.println("消费端Payload: " + message.getPayload());
		byte[] body= (byte[]) message.getPayload();
		String msg = new String(body, StandardCharsets.UTF_8);
		//对消息的处理
		readFile(msg,"utf-8");
		readFile2(msg);
//		byte[] bytes = toByteArrayNIO(msg);
//		String s=new String(bytes);
//		log.info(s);
		log.info("==============================nihao====================================");

		byte[] bytes1 = readFileTo(msg, "utf-8");
		String ss=new String(bytes1);
		log.info("bytes to String"+ss);
		Long deliveryTag = (Long)message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
		//手工ACK
		channel.basicAck(deliveryTag, false);
	}


	public static byte[] toByteArray(String filename) throws IOException {
		File f = new File(filename);
		if (!f.exists()) {
			throw new FileNotFoundException(filename);
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream((int) f.length());
		BufferedInputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(f));
			int buf_size = 1024;
			byte[] buffer = new byte[buf_size];
			int len = 0;
			while (-1 != (len = in.read(buffer, 0, buf_size))) {
				bos.write(buffer, 0, len);
			}
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			bos.close();
		}
	}


	public static byte[] toByteArrayNIO(String filename) throws IOException {
		File file = new File(filename);
		if (!file.exists()) {
			throw new FileNotFoundException(filename);
		}
		FileChannel channel = null;
		FileInputStream fs = null;
		try {
			fs = new FileInputStream(file);
			channel = fs.getChannel();
			ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size());
			while (channel.read(byteBuffer) > 0) {
				// do nothing
				// System.out.println("reading");
			}
			return byteBuffer.array();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 读取文件，使用字符编码读取文件，防止乱码
	 * 字符编码参数如果为空或者null，则使用默认读取文件
	 *
	 * @param filePath 文件路径
	 * @param encodingStr 字符编码
	 */
	public static void readFile(String filePath, String encodingStr) {
		FileInputStream fis = null;
		BufferedReader br = null;

		String line = null;
		try {
			File file=new File(filePath);
			fis = new FileInputStream(filePath);
			// 使用字符编码读取文件，防止乱码
			br = new BufferedReader(new InputStreamReader(fis, encodingStr));
			StringBuffer txt = new StringBuffer();
			while (null != (line = br.readLine())) {
				txt.append(line);
			}
			writeFile("D:\\queryFiles\\edam\\aa.txt",txt.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 释放资源
			if (null != fis) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static byte[] readFileTo(String filePath, String encodingStr) {
		FileInputStream fis = null;
		BufferedReader br = null;
		try {
			File file=new File(filePath);
			fis = new FileInputStream(file);
			// 使用字符编码读取文件，防止乱码
			br = new BufferedReader(new InputStreamReader(fis, encodingStr));
			long len = file.length();
			byte[] bytes = new byte[(int)len];
			FileInputStream in = null;
			try {
				in = new FileInputStream(file);
				int readLength = in.read(bytes);
				if ((long)readLength <len) {

				}
			} catch (Exception var10) {
				throw new RuntimeException("File is larger then max array size");
			} finally {

			}
			return bytes;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 释放资源
			if (null != fis) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	/**
	 * 写入Txt
	 * @param path
	 * @param txt
	 * @throws IOException
	 */
	public static boolean writeFile(String path, String txt) {
		// 相对路径，如果没有则要建立一个新的path文件
		File file = new File(path);
		try {
			// 创建新文件
			file.createNewFile();
			// 字符缓冲输出流：写东西到该文件
			BufferedWriter out = new BufferedWriter(new FileWriter(file));
			// 写东西：\r\n即为换行
			out.write(txt);
			// 把缓存区内容压入文件
			out.flush();
			// 最后关闭流
			out.close();
			//返回成功
			return true;
		} catch (IOException e) {
			//e.printStackTrace();
			//返回失败
			return false;
		}
	}
	public static void readFile2(String filePath) {
		FileInputStream fis = null;
		BufferedReader br = null;

		String line = null;
		try {
			fis = new FileInputStream(filePath);
			// 使用字符编码读取文件，防止乱码
			br = new BufferedReader(new InputStreamReader(fis));
			StringBuffer txt = new StringBuffer();
			while (null != (line = br.readLine())) {
				txt.append(line);

			}
			writeFile("D:\\queryFiles\\edam\\bb.txt",txt.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 释放资源
			if (null != fis) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static byte[] readFile3(String filePath) {
		FileInputStream fis = null;
		BufferedReader br = null;
		String line = null;
		try {
			File file=new File(filePath);
			fis = new FileInputStream(filePath);
			// 使用字符编码读取文件，防止乱码
			br = new BufferedReader(new InputStreamReader(fis));
			byte[] bytes = new byte[(int)br.read()];
			FileInputStream in = null;
			try {
				in = new FileInputStream(file);
				int readLength = in.read(bytes);
				if ((long)readLength < (int)br.read()) {

				}
			} catch (Exception var10) {
				throw new RuntimeException("File is larger then max array size");
			} finally {

			}
			return bytes;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 释放资源
			if (null != fis) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
}
