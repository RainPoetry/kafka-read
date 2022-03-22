package org.apache.kafka.cc.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;


public class Demo {

	private static final Logger log = LoggerFactory.getLogger(Demo.class);

	public void deal() {
		// 缩进4个空格
		String say = "hello";
		// 运算符的左右必须有一个空格
		int flag = 0;
		// 关键词if与括号之间必须有一个空格，括号内的f与左括号，0与右括号不需要空格
		if (flag == 0) {
			System.out.println(say);
		}
		// 左大括号前加空格且不换行；左大括号后换行
		if (flag == 1) {
			System.out.println("world");
			// 右大括号前换行，右大括号后有else，不用换行
		} else {
			System.out.println("ok");
		}

	}

	private static String fileName = "F:\\tmp\\kafka\\kafka-logs\\demo-0\\00000000000000000000.index";

	public static void main(String[] args) throws IOException {
		RandomAccessFile file = new RandomAccessFile(fileName, "rw");
		FileChannel fileChannel = file.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
		fileChannel.read(buffer);
		for (int i = 0; i < 30; i++) {
			System.out.println(buffer.getInt(i*8)+"-"+buffer.getInt(i*8+4) + " | " + i);
		}
	}

}
