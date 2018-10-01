package org.lzy.kaggle.googleAnalytics

object CaseClass {

  case class Customer(
                       //用户通过什么渠道进入商店，共8种，Organic站42%， social 25% 。。。
                       channelGrouping: String,
                       //访问商店的时间，20171016
                       date: String,
                       //用户的唯一标识符
                       fullVisitorId: String,
                       //访问商店行为的唯一识别符，有大约1k条的重复，所以不是唯一
                       sessionId: String,
                       visitId: Int,
                       //会话的号码，如果是第一个会话，使用1，大部分为1，有极端值，最大395
                       visitNumber: Double,
                       //时间戳，毫秒值
                       visitStartTime: Long,



                       //访问商店的设备
                       //浏览器
                       device_browser: String,
                       //通过什么方式访问、桌面还是移动
                       device_deviceCategory: String,
                       //是否是移动设备
                       device_isMobile: Double,
                       //操作系统
                       device_operatingSystem: String,



                       //用户的地理位置相关信息,是一个长json，最多的站7%
                       //城市
                       geoNetwork_city: String,
                       //大陆
                       geoNetwork_continent: String,
                       //国家
                       geoNetwork_country: String,
                       //地铁
                       geoNetwork_metro: String,
                       //网址
                       geoNetwork_networkDomain: String,
                       //区域
                       geoNetwork_region: String,
                       //大陆方向
                       geoNetwork_subContinent: String,




                       //用户的参与类型，社交参与/非社交参与。Socially Engaged" or "Not Socially Engaged
                       //所有数据中都为非社交参与
                       //                       socialEngagementType: String,
                       //总值，是一个json字符串，表示对一些特定值的访问次数，大约百分之40%的数据每一项访问了一次。
                       //	{"visits": "1", "hits": "4", "pageviews": "4"}  测试机
                       //  {"visits": "1", "hits": "5", "pageviews": "5", "newVisits": "1"}  训练集
                       //被拒绝总次数
                       totals_bounces: Option[Double],
                       //单曲总数
                       totals_hits: Double,
                       //新访问总数
                       totals_newVisits: Option[Double],
                       //网页浏览次数
                       totals_pageviews: Option[Double],
                       //交易总收入
                       totals_transactionRevenue: Option[Double],


                       //会话流量来源的相关信息，json字符串，
                       // {"campaign": "(not set)", "source": "google", "medium": "organic", "keyword": "(not provided)", "adwordsClickInfo": {"criteriaParameters": "not available in demo dataset"}, "isTrueDirect": true}
                       //会话的标志服，对用户而言是唯一的，全局唯一的应该使用fullVisitorId和visitId的组合
                       //广告目录
                       trafficSource_adContent: Option[String],
                       //
                       trafficSource_campaign: String,
                       //是否直接访问
                       trafficSource_isTrueDirect: Option[Double],
                       //
                       trafficSource_keyword: Option[String],
                       //中值
                       trafficSource_medium: String,
                       //介绍路劲
                       trafficSource_referralPath: Option[String],
                       //来源 baidu/google
                       trafficSource_source: String
                     )

}
