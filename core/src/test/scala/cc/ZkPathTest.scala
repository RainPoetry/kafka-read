package cc

import kafka.zookeeper.{GetDataRequest, GetDataResponse}
import org.apache.zookeeper.AsyncCallback.DataCallback
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import org.junit.Test

/**
  * User: chenchong
  * Date: 2018/11/21
  * description:
  */
class ZkPathTest {

  @Test
  def getData : Unit= {
    var connectString = "172.18.1.0:2181,172.18.1.2:2181,172.18.1.3:2181/kafka/data"
    var zk = new ZooKeeper(connectString, 50000, null);
    var getdata = GetDataRequest("/consumers",null,null);
    zk.getData("/consumers", null, new DataCallback {
      override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
        println(new String(data))
    }, null)
  }

}
