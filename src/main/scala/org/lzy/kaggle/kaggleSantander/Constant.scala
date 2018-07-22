package org.lzy.kaggle.kaggleSantander

/**
  * Created by Administrator on 2018/7/3.
  */
object Constant {
  //  val basePath = "E:\\dataset\\Kaggle_Santander\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/Kaggle_Santander/"

  val featureFilterColumns_arr=Array("id","target")
  val lableCol="target"
  val predictionCol=""
  val featuresCol="features"
  val randomSeed=10L
  //f190486d6是最后一个时间戳
  val specialColumns_arr=
    Array("f190486d6", "58e2e02e6", "eeb9cd3aa", "9fd594eec", "6eef030c1", "15ace8c9f",
        "fb0f5dbfe", "58e056e12", "20aa07010", "024c577b9", "d6bb78916", "b43a7cfd5",
        "58232a6fb", "1702b5bf0", "324921c7b", "62e59a501", "2ec5b290f", "241f0f867",
        "fb49e4212", "66ace2992", "f74e8f13d", "5c6487af1", "963a49cdc", "26fc93eb7",
        "1931ccfdd", "703885424", "70feb1494", "491b9ee45", "23310aa6f", "e176a204a",
        "6619d81fc", "1db387535",
        "fc99f9426", "91f701ba2", "0572565c2", "190db8488", "adb64ff71", "c47340d97", "c5a231d81", "0ff32eb98")
//    Array("f190486d6","58e2e02e6","eeb9cd3aa","9fd594eec","6eef030c1","58e056e12")
//Array("f190486d6","58e2e02e6","eeb9cd3aa","9fd594eec","6eef030c1","15ace8c9f","fb0f5dbfe",
// "58e056e12","20aa07010","024c577b9","d6bb78916","b43a7cfd5","58232a6fb")
  /*
  对于不同模型的存储位置，路径应该放在这里。

   */

  /*
  计算每一列在测试集中只有一个值的情况
   */
//cols_with_onlyone_val = train.columns[train.nunique() == 1]
  val cols_with_onlyone_val =Array("d5308d8bc","c330f1a67","eeac16933","7df8788e8","5b91580ee","6f29fbbc7","46dafc868","ae41a98b6","f416800e9","6d07828ca","7ac332a1d","70ee7950a","833b35a7c","2f9969eab","8b1372217","68322788b","2288ac1a6","dc7f76962","467044c26","39ebfbfd9","9a5ff8c23","f6fac27c8","664e2800e","ae28689a2","d87dcac58","4065efbb6","f944d9d43","c2c4491d5","a4346e2e2","1af366d4f","cfff5b7c8","da215e99e","5acd26139","9be9c6cef","1210d0271","21b0a54cb","da35e792b","754c502dd","0b346adbd","0f196b049","b603ed95d","2a50e001c","1e81432e7","10350ea43","3c7c7e24c","7585fce2a","64d036163","f25d9935c","d98484125","95c85e227","9a5273600","746cdb817","6377a6293","7d944fb0c","87eb21c50","5ea313a8c","0987a65a1","2fb7c2443","f5dde409b","1ae50d4c3","2b21cd7d8","0db8a9272","804d8b55b","76f135fa6","7d7182143","f88e61ae6","378ed28e0","ca4ba131e","1352ddae5","2b601ad67","6e42ff7c7","22196a84c","0e410eb3d","992e6d1d3","90a742107","08b9ec4ae","d95203ded","58ad51def","9f69ae59f","863de8a31","be10df47c","f006d9618","a7e39d23d","5ed0abe85","6c578fe94","7fa4fcee9","5e0571f07","fd5659511","e06b9f40f","c506599c8","99de8c2dc","b05f4b229","5e0834175","eb1cc0d9c","b281a62b9","00fcf67e4","e37b65992","2308e2b29","c342e8709","708471ebf","f614aac15","15ecf7b68","3bfe540f1","7a0d98f3c","e642315a5","c16d456a7","0c9b5bcfa","b778ab129","2ace87cdd","697a566f0","97b1f84fc","34eff114b","5281333d7","c89f3ba7e","cd6d3c7e6","fc7c8f2e8","abbbf9f82","24a233e8f","8e26b560e","a28ac1049","504502ce1","d9a8615f3","4efd6d283","34cc56e83","93e98252a","2b6cef19e","c7f70a49b","0d29ab7eb","e4a0d39b7","a4d1a8409","bc694fc8f","3a36fc3a2","4ffba44d3","9bfdec4bc","66a866d2f","f941e9df7","e7af4dbf3","dc9a54a3e","748168a04","bba8ce4bb","ff6f62aa4","b06fe66ba","ae87ebc42","f26589e57","963bb53b1","a531a4bf0","9fc79985d","9350d55c1","de06e884c","fc10bdf18","e0907e883","c586d79a1","e15e1513d","a06067897","643e42fcb","217cd3838","047ebc242","9b6ce40cf","3b2c972b3","17a7bf25a","c9028d46b","9e0473c91","6b041d374","783c50218","19122191d","ce573744f","1c4ea481e","fbd6e0a0b","69831c049","b87e3036b","54ba515ee","a09ba0b15","90f77ec55","fb02ef0ea","3b0cccd29","fe9ed417c","589e8bd6f","17b5a03fd","80e16b49a","a3d5c2c2a","1bd3a4e92","611d81daa","3d7780b1c","113fd0206","5e5894826","cb36204f9","bc4e3d600","c66e2deb0","c25851298","a7f6de992","3f93a3272","c1b95c2ec","6bda21fee","4a64e56e7","943743753","20854f8bf","ac2e428a9","5ee7de0be","316423a21","2e52b0c6a","8bdf6bc7e","8f523faf2","4758340d5","8411096ec","9678b95b7","a185e35cc","fa980a778","c8d90f7d7","080540c81","32591c8b4","5779da33c","bb425b41e","01599af81","1654ab770","d334a588e","b4353599c","51b53eaec","2cc0fbc52","45ffef194","c15ac04ee","5b055c8ea","d0466eb58","a80633823","a117a5409","7ddac276f","8c32df8b3","e5649663e","6c16efbb8","9118fd5ca","ca8d565f1","16a5bb8d2","fd6347461","f5179fb9c","97428b646","f684b0a96","e4b2caa9f","2c2d9f267","96eb14eaf","cb2cb460c","86f843927","ecd16fc60","801c6dc8e","f859a25b8","ae846f332","2252c7403","fb9e07326","d196ca1fd","a8e562e8e","eb6bb7ce1","5beff147e","52b347cdc","4600aadcf","6fa0b9dab","43d70cc4d","408021ef8","e29d22b59")
  /**
    * 高相关特征：
    * f190486d6
    */
}
