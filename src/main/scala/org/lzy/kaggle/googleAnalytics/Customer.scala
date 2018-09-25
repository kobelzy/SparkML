package org.lzy.kaggle.googleAnalytics

class Customer {
case class Customer(
                   //用户通过什么渠道进入商店
                     channelGrouping:String,
                   //访问商店的时间
  date:String,
                   //访问商店的设备
  device:String,
                   //用户的唯一标识符
  fullVisitorId:String,
                   //用户的地理位置相关信息
  geoNetwork:String,
                   //访问商店行为的唯一识别符
  sessionId:String,
                   //用户的参与类型，社交参与/非社交参与。Socially Engaged" or "Not Socially Engaged
  socialEngagementType:String,
                   //总值
  totals:String,
                   //会话流量来源的相关信息
  trafficSource:String,
                   //会话的标志服，对用户而言是唯一的，全局唯一的应该使用fullVisitorId和visitId的组合
  visitId:String,
                   //会话的号码，如果是第一个会话，使用1
  visitNumber:String,
                   //时间戳，
  visitStartTime    :String

                   )
}
