package org.apache.kafka.cc.test;

import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import sun.rmi.runtime.Log;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: chenchong
 * Date: 2018/12/6
 * description:
 */
public class LogRead {

	private static final AtomicInteger count = new AtomicInteger(0);
	private static final String log = "F:\\tmp\\kafka\\kafka-logs\\demo-0\\00000000000000000000.log";

	public static void logFile() throws IOException {
		FileRecords fileRecords = FileRecords.open(new File(log), true, Integer.MAX_VALUE, false);
		// 迭代 log 文件的每一个 MemoryRecords
		fileRecords.batches().forEach(k->{
			System.out.println(k.baseOffset() +" - " + k.lastOffset()+" - " + count.addAndGet(1));
		});
	}

	public static void main(String[] args) throws IOException {
		logFile();
	}
}
