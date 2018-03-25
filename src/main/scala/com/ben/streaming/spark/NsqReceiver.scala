package com.ben.streaming.spark

// NSQ
import com.github.brainlag.nsq.NSQConfig
import com.github.brainlag.nsq.NSQConsumer
import com.github.brainlag.nsq.lookup.DefaultNSQLookup
import com.github.brainlag.nsq.NSQMessage
import com.github.brainlag.nsq.callbacks.NSQMessageCallback
import com.github.brainlag.nsq.callbacks.NSQErrorCallback
import com.github.brainlag.nsq.exceptions.NSQException


// Spark
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

// This project
import model._

class NsqReceiver(nsqConfig: NsqConfig)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var consumer: NSQConsumer = null

  private def createConsumer(): NSQConsumer = {
    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException) =
        log.error(s"Exception while consuming topic $nsqConfig.inTopicName", e)
    }

    val nsqCallback = new  NSQMessageCallback {
      override def message(msg: NSQMessage): Unit = {
        val validMsg = new String(msg.getMessage)
        store(validMsg)
        msg.finished()
      }
    }

    val lookup = new DefaultNSQLookup
    // use NSQLookupd
    lookup.addLookupAddress(nsqConfig.host, nsqConfig.lookupPort)
    val consumer = new NSQConsumer(lookup,
                                   nsqConfig.inTopicName,
                                   nsqConfig.channelName,
                                   nsqCallback,
                                   new NSQConfig(),
                                   errorCallback)
    consumer
  }

  def onStart()  : Unit = {
    if (consumer == null) {
      consumer = createConsumer()
      consumer.start()
    }
    else {
      logWarning("NSQ receiver being asked to start more then once with out close")
    }

  }

  def onStop() : Unit = {
    if (consumer != null){
      consumer.close()
      consumer = null
    }
    else {
      logWarning("NSQ receiver stopped")
    }
  }
}
