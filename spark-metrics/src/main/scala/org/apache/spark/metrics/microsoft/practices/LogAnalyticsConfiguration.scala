package org.apache.spark.metrics.microsoft.practices

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
//import com.databricks.dbutils_v1.DBUtilsHolder.dbutils.secrets
//import org.apache.spark.metrics.MetricsSystemAccessor

private[spark] trait LogAnalyticsConfiguration extends Logging {
  protected def getWorkspaceId: Option[String]
  protected def getWorkspaceKey: Option[String]
  protected def getLogType: String
  protected def getTimestampFieldName: Option[String]

  private val logTypeValidation = "[a-zA-Z]+".r
//  private val secretScopeAndKeyValidation = "^([a-zA-Z0-9_\\.-]{1,128})\\:([a-zA-Z0-9_\\.-]{1,128})$"
//    .r("scope", "key")

  val workspaceId: String = {
    val value = getWorkspaceId
    require (value.isDefined, "A Log Analytics Workspace ID is required")
    logInfo(s"Setting workspaceId to ${value.get}")
    value.get
  }

  val workspaceKey: String = {
    val value = getWorkspaceKey
    value match {
//      case Some(scopeAndKey) => {
//        secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {
//          case Some(x) => {
//            secrets.get(x.group("scope"), x.group("key"))
//          }
//          case None => scopeAndKey
//        }
//      }
      case Some(secret) => secret
      case None => throw new SparkException(s"A Log Analytics Workspace Key is required")
    }
    //require(value.isDefined, "A Log Analytics Workspace Key is required")
    //value.get
  }


  val logType: String = {
    val value = getLogType
    logTypeValidation.findFirstMatchIn(value) match {
      case Some(_) => {
        logInfo(s"Setting logType to $value")
        value
      }
      case None => throw new SparkException(s"Invalid logType '$value'.  Only alpha characters are allowed.")
    }
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
    logInfo(s"Setting timestampNameField to $value")
    value.orNull
  }
}

private[spark] object LogAnalyticsListenerConfiguration {
  private val CONFIG_PREFIX = "spark.logAnalytics"

  private[spark] val WORKSPACE_ID = CONFIG_PREFIX + ".workspaceId"

  // We'll name this secret so Spark will redact it.
  private[spark] val WORKSPACE_KEY = CONFIG_PREFIX + ".secret"

  private[spark] val LOG_TYPE = CONFIG_PREFIX + ".logType"

  private[spark] val DEFAULT_LOG_TYPE = "SparkListenerEvent"

  private[spark] val TIMESTAMP_FIELD_NAME = CONFIG_PREFIX + ".timestampFieldName"

  private[spark] val LOG_BLOCK_UPDATES = CONFIG_PREFIX + ".logBlockUpdates"

  private[spark] val DEFAULT_LOG_BLOCK_UPDATES = false
}

private[spark] class LogAnalyticsListenerConfiguration(sparkConf: SparkConf)
  extends LogAnalyticsConfiguration {
  import LogAnalyticsListenerConfiguration._

  override def getWorkspaceId: Option[String] = sparkConf.getOption(WORKSPACE_ID)

  override def getWorkspaceKey: Option[String] = sparkConf.getOption(WORKSPACE_KEY)

  override def getLogType: String = sparkConf.get(LOG_TYPE, DEFAULT_LOG_TYPE)

  override def getTimestampFieldName: Option[String] = sparkConf.getOption(TIMESTAMP_FIELD_NAME)

  def logBlockUpdates: Boolean = {
    val value = sparkConf.getBoolean(LOG_BLOCK_UPDATES, DEFAULT_LOG_BLOCK_UPDATES)
    logInfo(s"Setting logBlockUpdates to $value")
    value
  }
}

private[spark] object LogAnalyticsSinkConfiguration {
  private[spark] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[spark] val LOGANALYTICS_KEY_SECRET = "secret"
  private[spark] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[spark] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[spark] val LOGANALYTICS_KEY_PERIOD = "period"
  private[spark] val LOGANALYTICS_KEY_UNIT = "unit"

  private[spark] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetrics"
  private[spark] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[spark] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

private[spark] class LogAnalyticsSinkConfiguration(properties: Properties)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsSinkConfiguration._
  import scala.collection.JavaConverters._

  try {
    //val spark = org.apache.spark.sql.SparkSession.builder.getOrCreate
    //val conf = spark.conf.getAll
    val conf = new SparkConf()
    val prop = new Properties()
    prop.putAll(Map(conf.getAll:_*).asJava)
    logWarning("Worked!")
    for (entry <- prop.entrySet.asScala) {
      logWarning(s"${entry.getKey}=${entry.getValue}")
    }

    for (entry <- conf.getExecutorEnv) {
      logWarning(s"${entry._1}=${entry._2}")
    }

//    val testRegistry = MetricsSystemAccessor.registry
//    if (testRegistry == null) {
//      logWarning("testRegistry is null")
//    } else {
//      logWarning("testRegistry was accessed!")
//    }
  } catch {
    case e: Throwable => logError("Error with SparkSession", e)
  }
  override def getWorkspaceId: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_WORKSPACEID))

  override def getWorkspaceKey: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_SECRET))

  override def getLogType: String = properties.getProperty(LOGANALYTICS_KEY_LOGTYPE, LOGANALYTICS_DEFAULT_LOGTYPE)

  override def getTimestampFieldName: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_TIMESTAMPFIELD, null))

  val pollPeriod: Int = {
    val value = properties.getProperty(LOGANALYTICS_KEY_PERIOD, LOGANALYTICS_DEFAULT_PERIOD).toInt
    logInfo(s"Setting polling period to $value")
    value
  }

  val pollUnit: TimeUnit = {
    val value = TimeUnit.valueOf(
      properties.getProperty(LOGANALYTICS_KEY_UNIT, LOGANALYTICS_DEFAULT_UNIT).toUpperCase)
    logInfo(s"Setting polling unit to $value")
    value
  }
}
