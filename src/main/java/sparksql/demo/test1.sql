##sql 语句
insert overwrite table portal.adm_push_user_abtest_stat_cube_daily partition(day) select program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, 'all' as push_send_type_group, 'all' as push_send_type, 'all' as send_type_desc, sum(send_uv) as send_uv, sum(arrive_uv) as arrive_uv, sum(show_uv) as show_uv, sum(open_uv) as open_uv, sum(send_pv) as send_pv, sum(arrive_pv) as arrive_pv, sum(show_pv) as show_pv, sum(open_pv) as open_pv, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end as app_channel_group, user.day from ( select distinct day, deviceid, program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc from portal.ddm_push_user_dims_merge_daily where day = '20200706' union all select distinct day, deviceid, program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc from portal.ddm_push_abtest_dims_daily where day = '20200706' ) user left outer join ( select day, uv_u, os_version, init_channel_group, init_channel, count(DISTINCT case WHEN galaxy_send_times > 0 THEN uv_u else null end) as send_uv, count(DISTINCT case WHEN galaxy_arrive_times > 0 THEN uv_u else null end) as arrive_uv, count(DISTINCT case WHEN galaxy_show_times > 0 THEN uv_u else null end) as show_uv, count(DISTINCT case WHEN galaxy_open_times > 0 THEN uv_u else null end) as open_uv, sum(galaxy_send_times) as send_pv, sum(galaxy_arrive_times) as arrive_pv, sum(galaxy_show_times) as show_pv, sum(galaxy_open_times) as open_pv from portal.gdm_push_user_day_new where day = '20200706' and app_id in ('2x1kfBk63z','B6c7g2','2S5Wcx','8x46xF') group by day, uv_u, os_version, init_channel_group, init_channel ) index on user.day = index.day and user.deviceid = index.uv_u group by program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end, user.day having program is not null union all select program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, coalesce(push_send_type, push_send_type_group) as push_send_type, coalesce(send_type_desc, push_send_type_group) as send_type_desc, count(DISTINCT case WHEN send_pv > 0 THEN uv_u else null end) as send_uv, count(DISTINCT case WHEN arrive_pv > 0 THEN uv_u else null end) as arrive_uv, count(DISTINCT case WHEN show_pv > 0 THEN uv_u else null end) as show_uv, count(DISTINCT case WHEN open_pv > 0 THEN uv_u else null end) as open_uv, sum(send_pv) as send_pv, sum(arrive_pv) as arrive_pv, sum(show_pv) as show_pv, sum(open_pv) as open_pv, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end as app_channel_group, user.day from ( select distinct day, deviceid, program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc from portal.ddm_push_user_dims_merge_daily where day = '20200706' union all select distinct day, deviceid, program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc from portal.ddm_push_abtest_dims_daily where day = '20200706' ) user left outer join ( select uv_u, os_version, init_channel_group, init_channel, case when push_send_type in ('1','9') then '全量' when push_send_type in ('4','6','10','12', '22') then '个性化实时' when push_send_type in ('2','11','17') then '个性化非实时' when push_send_type in ('3','7','13','15','18','19','21') then '本地实时' when push_send_type in ('5','14','20') then '本地非实时' when push_send_type in ('16') then '其他' else 'other-all' end as push_send_type_group, push_send_type, case when push_send_type = '1' then '全量实时' when push_send_type = '2' then '个性化非实时' when push_send_type = '3' then '本地实时' when push_send_type = '4' then '个性化实时' when push_send_type = '5' then '本地非实时' when push_send_type = '6' then '个性化超实时' when push_send_type = '7' then '本地超实时' when push_send_type = '9' then '特殊全量' when push_send_type = '10' then '机选个性化实时' when push_send_type = '11' then '机选个性化非实时' when push_send_type = '12' then '机选个性化超实时' when push_send_type = '13' then '机选本地实时' when push_send_type = '14' then '机选本地非实时' when push_send_type = '15' then '机选本地超实时' when push_send_type = '16' then '运营活动类' when push_send_type = '17' then '机选视频池非实时' when push_send_type = '18' then '天气类实时' when push_send_type = '19' then '竞品抓取本地实时' when push_send_type = '20' then '竞品抓取本地非实时' when push_send_type = '21' then '竞品抓取本地超实时' when push_send_type = '22' then '热点追踪-个性化实时' else 'other-detail' end as send_type_desc, coalesce(sum(galaxy_send_times), 0) as send_pv, coalesce(sum(galaxy_arrive_times), 0) as arrive_pv, coalesce(sum(galaxy_show_times), 0) as show_pv, coalesce(sum(galaxy_open_times), 0) as open_pv, day from portal.gdm_push_user_day_new where day = '20200706' AND app_id IN ('8x46xF','B6c7g2','2S5Wcx','2x1kfBk63z') AND (CASE WHEN app_id IN ('8x46xF') AND device_model rlike '.*iPad.*' THEN 1<0 ELSE 1>0 END) group by uv_u, os_version, init_channel_group, init_channel, case when push_send_type in ('1','9') then '全量' when push_send_type in ('4','6','10','12', '22') then '个性化实时' when push_send_type in ('2','11','17') then '个性化非实时' when push_send_type in ('3','7','13','15','18','19','21') then '本地实时' when push_send_type in ('5','14','20') then '本地非实时' when push_send_type in ('16') then '其他' else 'other-all' end, push_send_type, case when push_send_type = '1' then '全量实时' when push_send_type = '2' then '个性化非实时' when push_send_type = '3' then '本地实时' when push_send_type = '4' then '个性化实时' when push_send_type = '5' then '本地非实时' when push_send_type = '6' then '个性化超实时' when push_send_type = '7' then '本地超实时' when push_send_type = '9' then '特殊全量' when push_send_type = '10' then '机选个性化实时' when push_send_type = '11' then '机选个性化非实时' when push_send_type = '12' then '机选个性化超实时' when push_send_type = '13' then '机选本地实时' when push_send_type = '14' then '机选本地非实时' when push_send_type = '15' then '机选本地超实时' when push_send_type = '16' then '运营活动类' when push_send_type = '17' then '机选视频池非实时' when push_send_type = '18' then '天气类实时' when push_send_type = '19' then '竞品抓取本地实时' when push_send_type = '20' then '竞品抓取本地非实时' when push_send_type = '21' then '竞品抓取本地超实时' when push_send_type = '22' then '热点追踪-个性化实时' else 'other-detail' end, day ) index on user.day = index.day and user.deviceid = index.uv_u group by program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, push_send_type, send_type_desc, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end, user.day grouping sets((program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, push_send_type, send_type_desc, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end, user.day), (program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, os_version, case when manu ='华为' and init_channel_group in ('huawei_total_cpi_news') then '华为预装渠道' when manu ='华为' and init_channel_group in ('huawei_store') then '华为商店渠道' when manu ='华为' then '华为其他' when init_channel_group in ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') then '其他预装渠道' when init_channel_group in ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') then 'OV魅三小五商店渠道' when init_channel_group in ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') then '其他应用商店渠道' when init_channel_group in ('news_wap_dl') or init_channel='lite_wap_dl_28' then 'WAP渠道' when init_channel rlike 'netease_gw' then '官网渠道' when init_channel_group in ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') then '信息流渠道' when init_channel in ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') then '回流页渠道' when init_channel_group in ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') then 'SEM渠道' when lower(init_channel_group) in ('appstore_group') then 'APPStore渠道' when lower(platform) = 'ios' then 'iOS其他渠道' else '其他渠道' end, user.day)) having program is not null distribute by cast(rand() * 50 as bigint)


##分解后sql 一
SELECT program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc,
         'all' AS push_send_type_group,
         'all' AS push_send_type,
         'all' AS send_type_desc,
         sum(send_uv) AS send_uv,
         sum(arrive_uv) AS arrive_uv,
         sum(show_uv) AS show_uv,
         sum(open_uv) AS open_uv,
         sum(send_pv) AS send_pv,
         sum(arrive_pv) AS arrive_pv,
         sum(show_pv) AS show_pv,
         sum(open_pv) AS open_pv,
         os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道'
    END AS app_channel_group, user.day
FROM
(
    SELECT DISTINCT day,
         deviceid,
         program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc
    FROM portal.ddm_push_user_dims_merge_daily // first
    WHERE day = '20200706'
    UNION all

    SELECT DISTINCT day,
         deviceid,
         program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc
    FROM portal.ddm_push_abtest_dims_daily // second
    WHERE day = '20200706'
) user ## 临时表一
left outer JOIN
(
	SELECT day,
         uv_u,
         os_version,
         init_channel_group,
         init_channel,

        count(DISTINCT CASE WHEN galaxy_send_times > 0 THEN uv_u ELSE NULL end) AS send_uv,
        count(DISTINCT CASE WHEN galaxy_arrive_times > 0 THEN uv_u ELSE NULL end) AS arrive_uv,
        count(DISTINCT CASE WHEN galaxy_show_times > 0 THEN uv_u ELSE NULL end) AS show_uv,
        count(DISTINCT CASE WHEN galaxy_open_times > 0 THEN uv_u ELSE NULL end) AS open_uv,
        sum(galaxy_send_times) AS send_pv,
        sum(galaxy_arrive_times) AS arrive_pv,
        sum(galaxy_show_times) AS show_pv,
        sum(galaxy_open_times) AS open_pv
    FROM portal.gdm_push_user_day_new
    WHERE day = '20200706'
            AND app_id IN ('2x1kfBk63z','B6c7g2','2S5Wcx','8x46xF')
    GROUP BY  day, uv_u, os_version, init_channel_group, init_channel
 ) index ## 临时表二
 		ON user.day = index.day
        AND user.deviceid = index.uv_u
GROUP BY  program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道' end, user.day
HAVING program is NOT null


## 分解后sql 二
SELECT program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc,
         push_send_type_group,
         coalesce(push_send_type, push_send_type_group) AS push_send_type,
         coalesce(send_type_desc, ush_send_type_group) AS send_type_desc,
         count(DISTINCT CASE WHEN send_pv > 0 THEN uv_u ELSE NULL end) AS send_uv,
         count(DISTINCT CASE WHEN arrive_pv > 0 THEN uv_u ELSE NULL end) AS arrive_uv,
         count(DISTINCT CASE WHEN show_pv > 0 THEN uv_u ELSE NULL end) AS show_uv,
         count(DISTINCT CASE WHEN open_pv > 0 THEN uv_u ELSE NULL end) AS open_uv,
    	sum(send_pv) AS send_pv,
    	sum(arrive_pv) AS arrive_pv,
    	sum(show_pv) AS show_pv,
    	sum(open_pv) AS open_pv,
    	os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道'
    END AS app_channel_group, user.day
FROM
(
	SELECT DISTINCT day,
         deviceid,
         program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc
    FROM portal.ddm_push_user_dims_merge_daily
    WHERE day = '20200706'
    UNION all

    SELECT DISTINCT day,
         deviceid,
         program,
         abtest_version_name,
         abtest_name,
         active_level,
         platform,
         country,
         province,
         city,
         app_version_latest,
         device_model,
         brand,
         manu,
         city_level,
         ifpush,
         syspush,
         user_model,
         uc
    FROM portal.ddm_push_abtest_dims_daily
    WHERE day = '20200706'
) user // 临时表三
left outer JOIN
(
	SELECT day,
		 uv_u,
         os_version,
         init_channel_group,
         init_channel,

        CASE WHEN push_send_type IN ('1','9') THEN '全量' WHEN push_send_type IN ('4','6','10','12', '22') THEN '个性化实时' WHEN push_send_type IN ('2','11','17') THEN '个性化非实时'  WHEN push_send_type IN ('3','7','13','15','18','19','21') THEN '本地实时' WHEN push_send_type IN ('5','14','20') THEN '本地非实时' WHEN push_send_type IN ('16') THEN '其他' ELSE 'other-all' END AS push_send_type_group,
        push_send_type,
        CASE
        WHEN push_send_type = '1' THEN
        '全量实时'
        WHEN push_send_type = '2' THEN
        '个性化非实时'
        WHEN push_send_type = '3' THEN
        '本地实时'
        WHEN push_send_type = '4' THEN
        '个性化实时'
        WHEN push_send_type = '5' THEN
        '本地非实时'
        WHEN push_send_type = '6' THEN
        '个性化超实时'
        WHEN push_send_type = '7' THEN
        '本地超实时'
        WHEN push_send_type = '9' THEN
        '特殊全量'
        WHEN push_send_type = '10' THEN
        '机选个性化实时'
        WHEN push_send_type = '11' THEN
        '机选个性化非实时'
        WHEN push_send_type = '12' THEN
        '机选个性化超实时'
        WHEN push_send_type = '13' THEN
        '机选本地实时'
        WHEN push_send_type = '14' THEN
        '机选本地非实时'
        WHEN push_send_type = '15' THEN
        '机选本地超实时'
        WHEN push_send_type = '16' THEN
        '运营活动类'
        WHEN push_send_type = '17' THEN
        '机选视频池非实时'
        WHEN push_send_type = '18' THEN
        '天气类实时'
        WHEN push_send_type = '19' THEN
        '竞品抓取本地实时'
        WHEN push_send_type = '20' THEN
        '竞品抓取本地非实时'
        WHEN push_send_type = '21' THEN
        '竞品抓取本地超实时'
        WHEN push_send_type = '22' THEN
        '热点追踪-个性化实时'
        ELSE 'other-detail'
        END AS send_type_desc,
        coalesce(sum(galaxy_send_times), 0) AS send_pv,
        coalesce(sum(galaxy_arrive_times), 0) AS arrive_pv,
        coalesce(sum(galaxy_show_times), 0) AS show_pv,
        coalesce(sum(galaxy_open_times), 0) AS open_pv
    FROM portal.gdm_push_user_day_new

    WHERE day = '20200706'
            AND app_id IN ('8x46xF','B6c7g2','2S5Wcx','2x1kfBk63z')
            AND (CASE
        WHEN app_id IN ('8x46xF')
            AND device_model rlike '.*iPad.*' THEN
        1<0
        ELSE 1>0 END)
    GROUP BY  uv_u, os_version, init_channel_group, init_channel,
        CASE
        WHEN push_send_type IN ('1','9') THEN
        '全量'
        WHEN push_send_type IN ('4','6','10','12', '22') THEN
        '个性化实时'
        WHEN push_send_type IN ('2','11','17') THEN
        '个性化非实时'
        WHEN push_send_type IN ('3','7','13','15','18','19','21') THEN
        '本地实时'
        WHEN push_send_type IN ('5','14','20') THEN
        '本地非实时'
        WHEN push_send_type IN ('16') THEN
        '其他'
        ELSE 'other-all' end, push_send_type,
        CASE
        WHEN push_send_type = '1' THEN
        '全量实时'
        WHEN push_send_type = '2' THEN
        '个性化非实时'
        WHEN push_send_type = '3' THEN
        '本地实时'
        WHEN push_send_type = '4' THEN
        '个性化实时'
        WHEN push_send_type = '5' THEN
        '本地非实时'
        WHEN push_send_type = '6' THEN
        '个性化超实时'
        WHEN push_send_type = '7' THEN
        '本地超实时'
        WHEN push_send_type = '9' THEN
        '特殊全量'
        WHEN push_send_type = '10' THEN
        '机选个性化实时'
        WHEN push_send_type = '11' THEN
        '机选个性化非实时'
        WHEN push_send_type = '12' THEN
        '机选个性化超实时'
        WHEN push_send_type = '13' THEN
        '机选本地实时'
        WHEN push_send_type = '14' THEN
        '机选本地非实时'
        WHEN push_send_type = '15' THEN
        '机选本地超实时'
        WHEN push_send_type = '16' THEN
        '运营活动类'
        WHEN push_send_type = '17' THEN
        '机选视频池非实时'
        WHEN push_send_type = '18' THEN
        '天气类实时'
        WHEN push_send_type = '19' THEN
        '竞品抓取本地实时'
        WHEN push_send_type = '20' THEN
        '竞品抓取本地非实时'
        WHEN push_send_type = '21' THEN
        '竞品抓取本地超实时'
        WHEN push_send_type = '22' THEN
        '热点追踪-个性化实时'
        ELSE 'other-detail' end, day
 ) index
    ON user.day = index.day
        AND user.deviceid = index.uv_u
GROUP BY  program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, push_send_type, send_type_desc, os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道' end, user.day grouping sets((program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, push_send_type, send_type_desc, os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道' end, user.day), (program, abtest_version_name, abtest_name, active_level, platform, country, province, city, app_version_latest, device_model, brand, manu, city_level, ifpush, syspush, user_model, uc, push_send_type_group, os_version,
    CASE
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_total_cpi_news') THEN
    '华为预装渠道'
    WHEN manu ='华为'
        AND init_channel_group IN ('huawei_store') THEN
    '华为商店渠道'
    WHEN manu ='华为' THEN
    '华为其他'
    WHEN init_channel_group IN ('lenovo_total_cpi_news','bbk_total_news','oppo_total_news','meizu_total_news','jinli_total_news','hammer_total_news','kp_total_news','nubia_total_news','zgyd_total_news','yd_total_news','nubia_total_news_cpi','dianxin_2015total_news','xiaomi_yuzhi') THEN
    '其他预装渠道'
    WHEN init_channel_group IN ('oppo_store','meizu_store','vivo_store','samsung_cpa_news','miliao_news_group') THEN
    'OV魅三小五商店渠道'
    WHEN init_channel_group IN ('lenovo_store','jinli_store','hammer_store','letv_store','kp_store','nubia_store','wostore','yos_store','360oscpd','hesense_store','360_news_group','QQ_news_group') THEN
    '其他应用商店渠道'
    WHEN init_channel_group IN ('news_wap_dl')
        OR init_channel='lite_wap_dl_28' THEN
    'WAP渠道'
    WHEN init_channel rlike 'netease_gw' THEN
    '官网渠道'
    WHEN init_channel_group IN ('guangdiantong_dianwo','guangdiantong_ruidao','guangdiantong_yike','guangdiantong_ios_group','guangdiantong_zyz_news','guangdiantong_lieying','baichuan_bobo','miaopai_beike','news_liebao_baichuan','liebao_yunrui','liebao_dongrun','liebao_tusi','liebao_hc','wifinewsfeed','wifi_kangyuan','baichuan_wifi','wfxx_news_group','lz_bd_dsp','lz_bd_dsp_ios','zbl_dsp','duiba_baichuan','tuia_rd','toutiao_getui','getui_toutiao_iOS','toutiao_iOS_qxgroup','kuaishou_kangyuan','kuaishou_jintuo','kuaishou_chengxia','kuaishou_wangyi','news_wanka','lite_youdao','kuaiyou_dsp','kuaiyou_dsp_ios','getui_dsp','uc_zhiwei','qutoutiao_huihuang','qtt_ios','lite_sougou','sougou_group','miliaosp_news','miliaosp2_news','miliaosp3_news','lite_miliaosp_news','lite_miliaosp2_news','zhihu_news','zuoyebang_ios_group','news_bilibili_liuxing') THEN
    '信息流渠道'
    WHEN init_channel IN ('news_sps_popup','QQ_wxz01_news','sps_hongbao','sps_live','sps_article','sps_wxz','sps_book','news_sps_wap','sps_video','sps','sps_live_sc','wap_sps','news_sps_wap','sps_live_sc','lite_worldcup_hd1','worldcup_hd1','lite_sougou_hd1','newsreader_news_sougou_hd1','news_sps_bianji','news_sps_articaljingbian','news_sps_zhuantijingbian','news_sps_clickplay','news_sps_datamap','news_sps_toutiao') THEN
    '回流页渠道'
    WHEN init_channel_group IN ('prefect_SEM','lz-bd','lz-sm','lz_bd_ios','lz_sm_ios','news_bdzbl_sem_group','news_sm_sem_group') THEN
    'SEM渠道'
    WHEN lower(init_channel_group) IN ('appstore_group') THEN
    'APPStore渠道'
    WHEN lower(platform) = 'ios' THEN
    'iOS其他渠道'
    ELSE '其他渠道' end, user.day))
HAVING program is NOT NULL distribute by cast(rand() * 50 AS bigint)


## 详细表信息
## portal.ddm_push_user_dims_merge_daily
## 807.2 M  hdfs://hz-cluster9/user/portal/BDM/PUSH/ddm_push_user_dims_merge_daily/day=20200709

## portal.ddm_push_abtest_dims_daily
## 8.1 G  hdfs://hz-cluster9/user/portal/BDM/PUSH/ddm_push_abtest_dims_daily/day=20200709

## portal.gdm_push_user_day_new
## 6.4 G  hdfs://hz-cluster9/user/portal/GDM/PUSH/GDM_PUSH_USER_DAY_NEW/day=20200709

