package org.lzy.kaggle.eloRecommendation

import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}

trait RecordFeatures {
  val target = FeatureBuilder.RealNN[Record].extract(_.target.toRealNN).asResponse

  val card_id = FeatureBuilder.ID[Record].extract(_.card_id.toID).asPredictor

  val first_active_month = FeatureBuilder.PickList[Record].extract(_.first_active_month.toPickList).asPredictor
  val feature_1 = FeatureBuilder.Integral[Record].extract(_.feature_1.toIntegral).asPredictor
  val feature_2 = FeatureBuilder.Integral[Record].extract(_.feature_2.toIntegral).asPredictor
  val feature_3 = FeatureBuilder.Integral[Record].extract(_.feature_3.toIntegral).asPredictor

  val new_installments_sum_stddev = FeatureBuilder.Real[Record].extract(_.new_installments_sum_stddev.toReal).asPredictor
  val new_installments_sum_avg = FeatureBuilder.Real[Record].extract(_.new_installments_sum_avg.toReal).asPredictor
  val new_installments_min_stddev = FeatureBuilder.Real[Record].extract(_.new_installments_min_stddev.toReal).asPredictor
  val new_installments_min_avg = FeatureBuilder.Real[Record].extract(_.new_installments_min_avg.toReal).asPredictor
  val new_installments_max_stddev = FeatureBuilder.Real[Record].extract(_.new_installments_max_stddev.toReal).asPredictor
  val new_installments_max_avg = FeatureBuilder.Real[Record].extract(_.new_installments_max_avg.toReal).asPredictor
  val new_installments_std_stddev = FeatureBuilder.Real[Record].extract(_.new_installments_std_stddev.toReal).asPredictor
  val new_installments_std_avg = FeatureBuilder.Real[Record].extract(_.new_installments_std_avg.toReal).asPredictor
  val new_purchase_amount_sum_stddev = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_sum_stddev.toReal).asPredictor
  val new_purchase_amount_sum_avg = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_sum_avg.toReal).asPredictor
  val new_purchase_amount_min_stddev = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_min_stddev.toReal).asPredictor
  val new_purchase_amount_min_avg = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_min_avg.toReal).asPredictor
  val new_purchase_amount_max_stddev = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_max_stddev.toReal).asPredictor
  val new_purchase_amount_max_avg = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_max_avg.toReal).asPredictor
  val new_purchase_amount_std_stddev = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_std_stddev.toReal).asPredictor
  val new_purchase_amount_std_avg = FeatureBuilder.Real[Record].extract(_.new_purchase_amount_std_avg.toReal).asPredictor
  val new_category_1_avg = FeatureBuilder.Real[Record].extract(_.new_category_1_avg.toReal).asPredictor
  val new_category_1_std = FeatureBuilder.Real[Record].extract(_.new_category_1_std.toReal).asPredictor
  val new_category_2_avg = FeatureBuilder.Real[Record].extract(_.new_category_2_avg.toReal).asPredictor
  val new_category_2_std = FeatureBuilder.Real[Record].extract(_.new_category_2_std.toReal).asPredictor
  val new_city_id_count = FeatureBuilder.Integral[Record].extract(_.new_city_id_count.toIntegral).asPredictor
  val new_state_id_count = FeatureBuilder.Integral[Record].extract(_.new_state_id_count.toIntegral).asPredictor
  val new_subsector_id_count = FeatureBuilder.Integral[Record].extract(_.new_subsector_id_count.toIntegral).asPredictor
  val new_month_lag_avg = FeatureBuilder.Real[Record].extract(_.new_month_lag_avg.toReal).asPredictor
  val new_diff_month_avg = FeatureBuilder.Real[Record].extract(_.new_diff_month_avg.toReal).asPredictor
  val new_count_month = FeatureBuilder.Real[Record].extract(_.new_count_month.toReal).asPredictor
  val new_week_avg = FeatureBuilder.Real[Record].extract(_.new_week_avg.toReal).asPredictor
  val new_purchase_date_max = FeatureBuilder.DateTime[Record].extract(_.new_purchase_date_max.toDateTime).asPredictor
  val new_purchase_date_min = FeatureBuilder.DateTime[Record].extract(_.new_purchase_date_min.toDateTime).asPredictor
  val new_purchase_date_list = FeatureBuilder.DateList[Record].extract(_.new_purchase_date_list.getOrElse(Seq.empty[Long]).toDateList).asPredictor




  val auth_installments_sum_stddev = FeatureBuilder.Real[Record].extract(_.auth_installments_sum_stddev.toReal).asPredictor
  val auth_installments_sum_avg = FeatureBuilder.Real[Record].extract(_.auth_installments_sum_avg.toReal).asPredictor
  val auth_installments_min_stddev = FeatureBuilder.Real[Record].extract(_.auth_installments_min_stddev.toReal).asPredictor
  val auth_installments_min_avg = FeatureBuilder.Real[Record].extract(_.auth_installments_min_avg.toReal).asPredictor
  val auth_installments_max_stddev = FeatureBuilder.Real[Record].extract(_.auth_installments_max_stddev.toReal).asPredictor
  val auth_installments_max_avg = FeatureBuilder.Real[Record].extract(_.auth_installments_max_avg.toReal).asPredictor
  val auth_installments_std_stddev = FeatureBuilder.Real[Record].extract(_.auth_installments_std_stddev.toReal).asPredictor
  val auth_installments_std_avg = FeatureBuilder.Real[Record].extract(_.auth_installments_std_avg.toReal).asPredictor
  val auth_purchase_amount_sum_stddev = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_sum_stddev.toReal).asPredictor
  val auth_purchase_amount_sum_avg = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_sum_avg.toReal).asPredictor
  val auth_purchase_amount_min_stddev = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_min_stddev.toReal).asPredictor
  val auth_purchase_amount_min_avg = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_min_avg.toReal).asPredictor
  val auth_purchase_amount_max_stddev = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_max_stddev.toReal).asPredictor
  val auth_purchase_amount_max_avg = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_max_avg.toReal).asPredictor
  val auth_purchase_amount_std_stddev = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_std_stddev.toReal).asPredictor
  val auth_purchase_amount_std_avg = FeatureBuilder.Real[Record].extract(_.auth_purchase_amount_std_avg.toReal).asPredictor
  val auth_category_1_avg = FeatureBuilder.Real[Record].extract(_.auth_category_1_avg.toReal).asPredictor
  val auth_category_1_std = FeatureBuilder.Real[Record].extract(_.auth_category_1_std.toReal).asPredictor
  val auth_category_2_avg = FeatureBuilder.Real[Record].extract(_.auth_category_2_avg.toReal).asPredictor
  val auth_category_2_std = FeatureBuilder.Real[Record].extract(_.auth_category_2_std.toReal).asPredictor
  val auth_city_id_count = FeatureBuilder.Integral[Record].extract(_.auth_city_id_count.toIntegral).asPredictor
  val auth_state_id_count = FeatureBuilder.Integral[Record].extract(_.auth_state_id_count.toIntegral).asPredictor
  val auth_subsector_id_count = FeatureBuilder.Integral[Record].extract(_.auth_subsector_id_count.toIntegral).asPredictor
  val auth_month_lag_avg = FeatureBuilder.Real[Record].extract(_.auth_month_lag_avg.toReal).asPredictor
  val auth_diff_month_avg = FeatureBuilder.Real[Record].extract(_.auth_diff_month_avg.toReal).asPredictor
  val auth_count_month = FeatureBuilder.Real[Record].extract(_.auth_count_month.toReal).asPredictor
  val auth_week_avg = FeatureBuilder.Real[Record].extract(_.auth_week_avg.toReal).asPredictor
  val auth_purchase_date_max = FeatureBuilder.DateTime[Record].extract(_.auth_purchase_date_max.toDateTime).asPredictor
  val auth_purchase_date_min = FeatureBuilder.DateTime[Record].extract(_.auth_purchase_date_min.toDateTime).asPredictor
  val auth_purchase_date_list = FeatureBuilder.DateList[Record].extract(_.auth_purchase_date_list.getOrElse(Seq.empty[Long]).toDateList).asPredictor




  val hist_installments_sum_stddev = FeatureBuilder.Real[Record].extract(_.hist_installments_sum_stddev.toReal).asPredictor
  val hist_installments_sum_avg = FeatureBuilder.Real[Record].extract(_.hist_installments_sum_avg.toReal).asPredictor
  val hist_installments_min_stddev = FeatureBuilder.Real[Record].extract(_.hist_installments_min_stddev.toReal).asPredictor
  val hist_installments_min_avg = FeatureBuilder.Real[Record].extract(_.hist_installments_min_avg.toReal).asPredictor
  val hist_installments_max_stddev = FeatureBuilder.Real[Record].extract(_.hist_installments_max_stddev.toReal).asPredictor
  val hist_installments_max_avg = FeatureBuilder.Real[Record].extract(_.hist_installments_max_avg.toReal).asPredictor
  val hist_installments_std_stddev = FeatureBuilder.Real[Record].extract(_.hist_installments_std_stddev.toReal).asPredictor
  val hist_installments_std_avg = FeatureBuilder.Real[Record].extract(_.hist_installments_std_avg.toReal).asPredictor
  val hist_purchase_amount_sum_stddev = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_sum_stddev.toReal).asPredictor
  val hist_purchase_amount_sum_avg = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_sum_avg.toReal).asPredictor
  val hist_purchase_amount_min_stddev = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_min_stddev.toReal).asPredictor
  val hist_purchase_amount_min_avg = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_min_avg.toReal).asPredictor
  val hist_purchase_amount_max_stddev = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_max_stddev.toReal).asPredictor
  val hist_purchase_amount_max_avg = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_max_avg.toReal).asPredictor
  val hist_purchase_amount_std_stddev = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_std_stddev.toReal).asPredictor
  val hist_purchase_amount_std_avg = FeatureBuilder.Real[Record].extract(_.hist_purchase_amount_std_avg.toReal).asPredictor
  val hist_category_1_avg = FeatureBuilder.Real[Record].extract(_.hist_category_1_avg.toReal).asPredictor
  val hist_category_1_std = FeatureBuilder.Real[Record].extract(_.hist_category_1_std.toReal).asPredictor
  val hist_category_2_avg = FeatureBuilder.Real[Record].extract(_.hist_category_2_avg.toReal).asPredictor
  val hist_category_2_std = FeatureBuilder.Real[Record].extract(_.hist_category_2_std.toReal).asPredictor
  val hist_city_id_count = FeatureBuilder.Integral[Record].extract(_.hist_city_id_count.toIntegral).asPredictor
  val hist_state_id_count = FeatureBuilder.Integral[Record].extract(_.hist_state_id_count.toIntegral).asPredictor
  val hist_subsector_id_count = FeatureBuilder.Integral[Record].extract(_.hist_subsector_id_count.toIntegral).asPredictor
  val hist_month_lag_avg = FeatureBuilder.Real[Record].extract(_.hist_month_lag_avg.toReal).asPredictor
  val hist_diff_month_avg = FeatureBuilder.Real[Record].extract(_.hist_diff_month_avg.toReal).asPredictor
  val hist_count_month = FeatureBuilder.Real[Record].extract(_.hist_count_month.toReal).asPredictor
  val hist_week_avg = FeatureBuilder.Real[Record].extract(_.hist_week_avg.toReal).asPredictor
  val hist_purchase_date_max = FeatureBuilder.DateTime[Record].extract(_.hist_purchase_date_max.toDateTime).asPredictor
  val hist_purchase_date_min = FeatureBuilder.DateTime[Record].extract(_.hist_purchase_date_min.toDateTime).asPredictor
  val hist_purchase_date_list = FeatureBuilder.DateList[Record].extract(_.hist_purchase_date_list.getOrElse(Seq.empty[Long]).toDateList).asPredictor




  val features = Seq(card_id, first_active_month, feature_1, feature_2, feature_3,
    new_installments_sum_stddev, new_installments_sum_avg, new_installments_min_stddev, new_installments_min_avg, new_installments_max_stddev, new_installments_max_avg, new_installments_std_stddev, new_installments_std_avg,
    new_purchase_amount_sum_stddev, new_purchase_amount_sum_avg, new_purchase_amount_min_stddev, new_purchase_amount_min_avg, new_purchase_amount_max_stddev, new_purchase_amount_max_avg, new_purchase_amount_std_stddev, new_purchase_amount_std_avg,
    new_category_1_avg, new_category_1_std, new_category_2_avg, new_category_2_std, new_city_id_count, new_state_id_count, new_subsector_id_count, new_month_lag_avg, new_diff_month_avg, new_count_month, new_week_avg,
    new_purchase_date_max, new_purchase_date_min,new_purchase_date_list,

    auth_installments_sum_stddev, auth_installments_sum_avg, auth_installments_min_stddev, auth_installments_min_avg, auth_installments_max_stddev, auth_installments_max_avg, auth_installments_std_stddev, auth_installments_std_avg,
    auth_purchase_amount_sum_stddev, auth_purchase_amount_sum_avg, auth_purchase_amount_min_stddev, auth_purchase_amount_min_avg, auth_purchase_amount_max_stddev, auth_purchase_amount_max_avg, auth_purchase_amount_std_stddev, auth_purchase_amount_std_avg,
    auth_category_1_avg, auth_category_1_std, auth_category_2_avg, auth_category_2_std, auth_city_id_count, auth_state_id_count, auth_subsector_id_count, auth_month_lag_avg, auth_diff_month_avg, auth_count_month, auth_week_avg,
    auth_purchase_date_max, auth_purchase_date_min,auth_purchase_date_list,

    hist_installments_sum_stddev, hist_installments_sum_avg, hist_installments_min_stddev, hist_installments_min_avg, hist_installments_max_stddev, hist_installments_max_avg, hist_installments_std_stddev, hist_installments_std_avg,
    hist_purchase_amount_sum_stddev, hist_purchase_amount_sum_avg, hist_purchase_amount_min_stddev, hist_purchase_amount_min_avg, hist_purchase_amount_max_stddev, hist_purchase_amount_max_avg, hist_purchase_amount_std_stddev, hist_purchase_amount_std_avg,
    hist_category_1_avg, hist_category_1_std, hist_category_2_avg, hist_category_2_std, hist_city_id_count, hist_state_id_count, hist_subsector_id_count, hist_month_lag_avg, hist_diff_month_avg, hist_count_month, hist_week_avg,
    hist_purchase_date_max, hist_purchase_date_min,hist_purchase_date_list

  ).transmogrify()
}
