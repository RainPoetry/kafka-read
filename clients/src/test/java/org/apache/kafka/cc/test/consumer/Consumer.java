package org.apache.kafka.cc.test.consumer;

import java.util.Collections;

/**
 * User: chenchong
 * Date: 2018/11/15
 * description:
 */
public class Consumer {

	/**
	 * 1. 不是线程安全
	 * 2. 多线程： 1个线程一个 Consumer 实例
	 */

	public static void main(String[] args){
		System.out.println("cc".hashCode()%50);
	}
}
