INSERT INTO default.dm_sr_kpi_search_2
SELECT
	{date:String} AS date_str,
    tt0.app_id AS platform,
	tt0.search_word AS search_word,
	tt0.search_type AS search_type,

	tt1.search_uv AS search_uv,
	tt1.search_pv AS search_pv,
	tt1.search_times AS search_times,
	tt2.click_uv AS click_uv,
	tt2.click_times AS click_times,
	tt3.create_cust_num AS create_cust_num,
	tt3.create_parent_num AS create_parent_num,
    tt3.create_quantity_num AS create_quantity_num,
	tt3.create_sale_amt AS create_sale_amt,
	tt4.no_search_times AS no_search_times,
	tt4.no_search_uv AS no_search_uv,
	tt5.search_no_click AS search_no_click,
    tt6.median_sku_max_ex_location AS median_sku_max_ex_location,
    tt7.median_sku_max_cl_location AS median_sku_max_cl_location,
	tt6.avg_sku_max_ex_location AS avg_sku_max_ex_location,
	tt7.avg_sku_max_cl_location AS avg_sku_max_cl_location,
    tt8.next_search_word_time AS next_search_word_time,

    tt3.zf_prod_sale_amt AS zf_prod_sale_amt,
    tt3.out_pay_amount AS out_pay_amount
FROM
-- 所有词
	(
		SELECT
			app_id, search_type, search_word
		FROM
			default.tp_dm_sr_kpi_base_2
		group by app_id, search_type, search_word
	) tt0
LEFT JOIN
-- 搜索
	(
	SELECT
		app_id, search_type, search_word, COUNT(1) AS search_pv, COUNT(DISTINCT udid) AS search_uv, COUNT(1) AS search_times
	FROM
		default.tp_dm_sr_kpi_base_2
	WHERE
		event_id = 4311
	GROUP BY
		app_id, search_type, search_word
    ) tt1
ON tt0.app_id = tt1.app_id AND tt0.search_type = tt1.search_type AND tt0.search_word = tt1.search_word
LEFT JOIN
-- 点击
	(
	SELECT
	    t1.app_id AS app_id, t2.search_type AS search_type, t1.search_word AS search_word, COUNT(DISTINCT t1.udid, t1.creation_date, t1.main_product_id) AS click_times, COUNT(DISTINCT t1.udid) AS click_uv
	FROM
	    (
	    SELECT
            app_id, search_type, search_word,  udid, last_cseq, creation_date, main_product_id
        FROM
            default.tp_dm_sr_kpi_base_2
        WHERE
            event_id IN (4201, 4031) AND LENGTH(last_cseq) > 0) t1
	LEFT JOIN
	    (
	    SELECT
            last_cseq, search_type
        FROM
            default.tp_dm_sr_kpi_base_2
        WHERE
            event_id = 4311) t2
    ON t1.last_cseq = t2.last_cseq
    WHERE
    	t2.last_cseq <> ''
    GROUP BY
    	t1.app_id, t2.search_type, t1.search_word
    ) tt2
ON tt0.app_id = tt2.app_id AND tt0.search_type = tt2.search_type AND tt0.search_word = tt2.search_word
LEFT JOIN
-- 收订
	(
	SELECT
	     t1.app_id AS app_id, t2.search_type AS search_type, t1.search_word AS search_word, COUNT(DISTINCT t1.order_id) AS create_parent_num, SUM(t1.order_quantity) AS create_quantity_num, COUNT(DISTINCT t1.order_cust_id) AS create_cust_num, SUM(t1.bargin_price * t1.order_quantity) AS create_sale_amt
         , sum(zf_prod_sale_amt) as zf_prod_sale_amt, sum(out_pay_amount) as out_pay_amount
    FROM
        (
        SELECT
            app_id, search_type, search_word, order_cust_id, bargin_price, order_quantity, last_cseq, order_id
            , zf_prod_sale_amt, out_pay_amount
        FROM
            default.tp_dm_sr_kpi_base_2
        WHERE
            order_id >= 0 AND LENGTH(last_cseq) > 0) t1
    LEFT JOIN
        (
        SELECT
            last_cseq, search_type
        FROM
            default.tp_dm_sr_kpi_base_2
        WHERE
            event_id = 4311) t2
    ON t1.last_cseq = t2.last_cseq
    WHERE
        t2.last_cseq <> ''
    GROUP BY
    	t1.app_id, t2.search_type, t1.search_word
    ) tt3
ON tt0.app_id = tt3.app_id AND tt0.search_type = tt3.search_type AND tt0.search_word = tt3.search_word
LEFT JOIN
-- 无结果搜索
	(
	SELECT
		app_id, search_type, search_word, COUNT(1) AS no_search_times, COUNT(DISTINCT udid) AS no_search_uv
	FROM
		default.tp_dm_sr_kpi_base_2
	WHERE
		event_id = 4311 AND search_num = 0
	GROUP BY
		app_id, search_type, search_word
    ) tt4
 ON tt0.app_id = tt4.app_id AND tt0.search_word = tt4.search_word AND tt0.search_type = tt4.search_type
 LEFT JOIN
-- 无点击搜索
	(
	SELECT
		t1.search_word, t1.app_id, t1.search_type, COUNT(1) AS search_no_click
	FROM
		(
		SELECT
			search_word, app_id, search_type, last_cseq
		FROM
			default.tp_dm_sr_kpi_base_2
		WHERE
			event_id = 4311 AND LENGTH(last_cseq) > 0) t1
	LEFT JOIN (
		SELECT
			last_cseq
		FROM
			default.tp_dm_sr_kpi_base_2
		WHERE
			event_id IN (4201, 4031)) t2
	ON t1.last_cseq = t2.last_cseq
	WHERE
		t2.last_cseq = ''
	GROUP BY
		t1.app_id, t1.search_type, t1.search_word
    ) tt5
 ON tt0.app_id = tt5.app_id AND tt0.search_type = tt5.search_type AND tt0.search_word = tt5.search_word
 LEFT JOIN
 -- 平均/中位 曝光位置
 	(
	SELECT
	    t.app_id,
	    t.search_type,
        t.search_word,
	    ROUND(SUM(t.max_position)/ SUM(t.one)) + 1 AS avg_sku_max_ex_location,
        medianExact(0.5)(t.max_position) + 1 AS median_sku_max_ex_location
	FROM
	    (
        SELECT
            t1.app_id, t2.search_type AS search_type, t1.search_word, 1 AS one, MAX(t1.position) AS max_position
        FROM
            (
            SELECT
                app_id, search_type, search_word, position, last_cseq
            FROM
                default.tp_dm_sr_kpi_base_2
            WHERE
                event_id = 6403 AND position >= 0 AND LENGTH(last_cseq) > 0) t1
        LEFT JOIN
            (
            SELECT
                last_cseq, search_type
            FROM
                default.tp_dm_sr_kpi_base_2
            WHERE
                event_id = 4311) t2
        ON t1.last_cseq = t2.last_cseq
        WHERE
            t2.last_cseq <> ''
        GROUP BY
            t1.app_id, t2.search_type, t1.search_word, t1.last_cseq) t
    GROUP BY
		t.app_id, t.search_type, t.search_word
	) tt6
 ON tt0.app_id = tt6.app_id AND tt0.search_type = tt6.search_type AND tt0.search_word = tt6.search_word
 LEFT JOIN
-- 平均/中位 点击位置
 	(
	SELECT
	    t.app_id, t.search_type,
        t.search_word,
	    ROUND(SUM(t.max_position)/ SUM(t.one)) + 1 AS avg_sku_max_cl_location,
        medianExact(0.5)(t.max_position) + 1 AS median_sku_max_cl_location
	FROM
	    (
        SELECT
            t1.app_id, t2.search_type AS search_type, t1.search_word, 1 AS one, MAX(t1.position) AS max_position
        FROM
            (
            SELECT
                app_id, search_type, search_word, position, last_cseq
            FROM
                default.tp_dm_sr_kpi_base_2
            WHERE
                event_id IN (4201, 4031) AND position >= 0 AND LENGTH(last_cseq) > 0) t1
        LEFT JOIN
            (
            SELECT
                last_cseq, search_type
            FROM
                default.tp_dm_sr_kpi_base_2
            WHERE
                event_id = 4311) t2
        ON t1.last_cseq = t2.last_cseq
        WHERE
            t2.last_cseq <> ''
        GROUP BY
            t1.app_id, t2.search_type, t1.search_word, t1.last_cseq) t
    GROUP BY
		t.app_id, t.search_type, t.search_word
	) tt7
ON tt0.app_id = tt7.app_id AND tt0.search_type = tt7.search_type AND tt0.search_word = tt7.search_word
    LEFT JOIN
-- 下一个搜索词次数
    (
        SELECT platform AS app_id,
               current_search_word AS search_word,
               SUM(be_searched_time) AS next_search_word_time
        FROM default.dm_sr_kpi_next_search_word_2
        WHERE date_str = {date:String} AND app_id IN ('2', '3')
        GROUP BY date_str, current_search_word, app_id
        ) tt8
ON tt0.search_word = tt8.search_word AND tt0.app_id = tt8.app_id;