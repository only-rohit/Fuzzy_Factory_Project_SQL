DROP DATABASE IF EXISTS advanced_sql_course;
CREATE DATABASE advanced_sql_course;

DROP SCHEMA IF EXISTS mavenfuzzyfactory;
CREATE SCHEMA mavenfuzzyfactory;

CREATE TABLE website_sessions (
  website_session_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  user_id INT NOT NULL,
  is_repeat_session INT NOT NULL, 
  utm_source VARCHAR(12), 
  utm_campaign VARCHAR(20),
  utm_content VARCHAR(15), 
  device_type VARCHAR(15), 
  http_referer VARCHAR(30)
);

CREATE TABLE website_pageviews (
  website_pageview_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  website_session_id INT NOT NULL,
  pageview_url VARCHAR(50) NOT NULL
);

CREATE TABLE products (
  product_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  product_name VARCHAR(50) NOT NULL
);

CREATE TABLE orders (
  order_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  website_session_id INT NOT NULL,
  user_id INT NOT NULL,
  primary_product_id INT NOT NULL,
  items_purchased INT NOT NULL,
  price_usd DECIMAL(6,2) NOT NULL,
  cogs_usd DECIMAL(6,2) NOT NULL
);

CREATE TABLE order_items (
  order_item_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  is_primary_item INT NOT NULL,
  price_usd DECIMAL(6,2) NOT NULL,
  cogs_usd DECIMAL(6,2) NOT NULL
);

CREATE TABLE order_item_refunds (
  order_item_refund_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  order_item_id INT NOT NULL,
  order_id INT NOT NULL,
  refund_amount_usd DECIMAL(6,2) NOT NULL
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/website_sessions.csv' 
INTO TABLE website_sessions
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/website_pageviews.csv' 
INTO TABLE website_pageviews
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv' 
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/orders.csv' 
INTO TABLE orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/order_items.csv' 
INTO TABLE order_items
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/order_item_refunds.csv' 
INTO TABLE order_item_refunds
FIELDS TERMINATED BY ',' 
ENCLOSED BY ''''
LINES TERMINATED BY '\n'
IGNORE 0 ROWS;

----------------------TRAFFIC SOURCE ANALYSIS AND CHANNEL PORTFOLIO MANAGEMENT-------------------------

-- 1. Top Traffic Source Analysis (with Trending & CVRs)

-- 1.1 Pulling data on website sessions volume breakdown by UTM Source, UTM Campaign and referral domain.
SELECT
    utm_source,
    utm_campaign,
    http_referer,
    COUNT (website_session_id) AS sessions
FROM website_sessions
WHERE created_at < '2012-04-14'
GROUP BY 1,2,3
ORDER BY 4 DESC;

-- 1.2 So far, Gsearch seems to be the biggest driver for the business. Pulling monthly trends for
--     Gsearch sessions and orders to showcase the growth for the past 8 months.
SELECT
    YEAR(s.created_at) AS yr,
    MONTH(s.created_at) AS mo,
    COUNT (DISTINCT s.website_session_id) AS sessions,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT (DISTINCT o.order_id)/COUNT (DISTINCT s.website_session_id) AS conv_rate
FROM website_sessions s LEFT JOIN orders o ON s.website_session_id = o.website_session_id
WHERE s.created_at < '2012-11-27' AND s.utm_source = 'gsearch'
GROUP BY 1,2;

-- 1.3 Pulling data to show a similar monthly trend for Gsearch, but this time splitting out nonbrand
-- and brand campaigns separately to evaluate if the brand is picking up.
SELECT
    YEAR (s.created_at) AS yr,
    MONTH (s.created_at) AS mo,
    COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN s.website_session_id ELSE NULL END) AS nonbrand_sessions,
    COUNT (DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN o.order_id ELSE NULL END) AS nonbrand_orders,
    COUNT (DISTINCT CASE WHEN utm_campaign = 'brand' THEN s.website_session_id ELSE NULL END) AS brand_sessions,
    COUNT (DISTINCT CASE WHEN utm_campaign = 'brand' THEN o.order_id ELSE NULL END) AS brand_orders
FROM website_sessions s LEFT JOIN orders o
    ON s.website_session_id = o.website_session_id
WHERE s.created_at < '2012-11-27' AND s.utm_source = 'gsearch'
GROUP BY 1,2;

--2. Bid Optimization for Paid Traffic
-- Dive deeper into the top driver Gsearch Nonbrand, pull monthly sessions and orders split by
-- device type to understand trended device-level performance.

SELECT
    YEAR(S.created_at) AS yr,
    MONTH (s.created_at) AS mo,
    COUNT (DISTINCT CASE WHEN device_type = 'desktop' THEN s.website_session_id ELSE NULL END) AS desktop_sessions,
    COUNT (DISTINCT CASE WHEN device_type = 'desktop' THEN o.order_id ELSE NULL END) AS desktop_orders,
    COUNT (DISTINCT CASE WHEN device_type = 'mobile' THEN s.website_session_id ELSE NULL END) AS mobile_sessions,
    COUNT (DISTINCT CASE WHEN device_type = 'mobile' THEN o.order_id ELSE NULL END) AS mobile_orders
FROM website_sessions s LEFT JOIN orders o
    ON s.website_session_id = o.website_session_id
WHERE s.created_at < '2012-11-27'
    AND s.utm_source ='gsearch'
    AND s.utm_campaign='nonbrand'
GROUP BY 1,2;

-- 3.Analysis for Channel Portfolio Management

-- 3.1 Multi-Channel Bidding: show nonbrand sessions, orders, and conversion rates from session
-- to order for Gsearch and Bsearch with a breakdown by device type to figure out if Bsearch nonbrand traffic
-- should have the same bids as Gsearch nonbrand.

SELECT
    s.device_type,
    s.utm_source,
    COUNT (DISTINCT s.website_session_id) AS sessions,
    COUNT (DISTINCT o.order_id) AS orders,
    COUNT (DISTINCT o.order_id)/COUNT (DISTINCT s.website_session_id) AS conv_rate
FROM website_sessions s LEFT JOIN orders o
    ON s.website_session_id = o.website_session_id
WHERE s.created_at > '2012-08-22'
    AND s.created_at < '2012-09-19'
    AND s.utm_campaign = 'nonbrand'
GROUP BY 1,2;

-- 3.2 Analyzing channel Portfolio Trends/ Impact of Bid Change
SELECT
    MIN (DATE (created_at)) AS week_start_date,
    COUNT(DISTINCT CASE WHEN utm_source='gsearch' AND device_type = 'desktop' THEN website_session_id
    ELSE NULL END) AS g_dtop_sessions,
    COUNT (DISTINCT CASE WHEN utm_source= 'bsearch' AND device_type='desktop' THEN website_session_id
    ELSE NULL END) AS b_dtop_sessions,
    COUNT (DISTINCT CASE WHEN utm_source= 'bsearch' AND device_type=' desktop' THEN website_session_id
    ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN utm_source='gsearch' AND device_type='desktop' THEN website_session_id
    ELSE NULL END) AS b_pct_of_g_dtop,
    COUNT(DISTINCT CASE WHEN utm_source='gsearch' AND device_type='mobile' THEN website_session_id ELSE
    NULL END) AS g_mob_sessions,
    COUNT(DISTINCT CASE WHEN utm_source='bsearch' AND device_type='mobile' THEN website_session_id ELSE
    NULL END) AS b_mob_sessions,
    COUNT (DISTINCT CASE WHEN utm_source='bsearch' AND device_type='mobile' THEN website_session_id ELSE
    NULL END)
        /COUNT (DISTINCT CASE WHEN utm_source='gsearch' AND device_type='mobile' THEN website_session_id
    ELSE NULL END) AS b_pct_of_g_mob
FROM website_sessions
WHERE created_at> '2012-11-04'
    AND created_at < '2012-12-22'
    AND utm_campaign = 'nonbrand'
GROUP BY YEARWEEK (created_at);

 -- 3.3 Site Traffic Breakdown
WITH t1 AS
(
SELECT
    website_session_id,
    created_at,
    CASE
        WHEN utm_source IS NULL AND http_referer IN ('https://www.gsearch.com', 'https://www.bsearch.com') THEN 'organic_search'
        WHEN utm_campaign= 'nonbrand' THEN 'paid_nonbrand'
        WHEN utm_campaign = 'brand' THEN 'paid_brand'
        WHEN utm_source IS NULL AND http_referer IS NULL THEN 'direct_type_in'
    END AS channel_group
FROM website_sessions
WHERE created_at <'2012-12-23'
)

SELECT 
    YEAR (created_at) AS yr,
    MONTH(created_at) AS mo,
    COUNT (DISTINCT CASE WHEN channel_group = 'paid_nonbrand' THEN website_session_id ELSE NULL END) AS non_brand,
    COUNT(DISTINCT CASE WHEN channel_group = 'paid_brand' THEN website_session_id ELSE NULL END) AS brand,
    COUNT (DISTINCT CASE WHEN channel_group = 'paid_brand' THEN website_session_id ELSE NULL END)
        /COUNT(DISTINCT CASE WHEN channel_group = 'paid_nonbrand' THEN website_session_id ELSE NULL END) AS
    brand_pct_of_nonbrand,
    COUNT(DISTINCT CASE WHEN channel_group = 'direct_type_in' THEN website_session_id ELSE NULL END) AS direct,
    COUNT (DISTINCT CASE WHEN channel_group = 'direct_type_in' THEN website_session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN channel_group = 'paid_nonbrand' THEN website_session_id ELSE NULL END) AS
    direct_pct_of_nonbrand,
    COUNT(DISTINCT CASE WHEN channel_group = 'organic_search' THEN website_session_id ELSE NULL END) AS organic,
    COUNT(DISTINCT CASE WHEN channel_group = 'organic_search' THEN website_session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN channel_group = 'paid_nonbrand' THEN website_session_id ELSE NULL END) AS
    organic_pct_of_nonbrand
FROM t1
GROUP BY 1,2;


------------------------------------ANALYZING WEBSITE PERFORMANCE-------------------------------------

-- 1. Identifying Top Website Pages & Top Entry Pages
-- 1.1 Most Viewed Pages
SELECT
    pageview_url,
    COUNT(DISTINCT website_pageview_id) AS pvs
FROM website_pageviews
WHERE created_at < '2012-06-09'
GROUP BY pageview_url
ORDER BY pvs DESC;

-- 1.2 Top Entry Pages
WITH first_pageview 
AS (
SELECT
website_session_id,
MIN (website_pageview_id) AS min_pageview_id 
FROM website_pageviews
WHERE created_at <'2012-06-12' 
GROUP BY 1) 
SELECT
t2.pageview_url AS landing_page,
COUNT (DISTINCT t1.website_session_id) AS sessions_hitting_this_lander
FROM first_pageview t1
LEFT JOIN website_pageviews t2
ON t1.min_pageview_id = t2.website_pageview_id
GROUP BY landing_page;

-- 2. Analyzing Landing Page Performance & Testing
-- 2.1 Calculating Bounce Rates & Analyzing Landing Page Tests
SELECT
MIN (created_at) AS first_created_at,
MIN(website_pageview_id) AS first_pageview_id
FROM website_pageviews
WHERE pageview_url ='/lander-1'
AND created_at IS NOT NULL;

WITH t1 AS
(
    SELECT
    wp.website_session_id AS website_session_id,
    MIN(wp.website_pageview_id) AS min_pageview_id
FROM website_pageviews wp JOIN website_sessions ws
    ON wp.website_session_id = ws.website_session_id
    AND ws.created_at < '2012-07-28' 
    AND wp.website_pageview_id > 23504 
    AND ws.utm_source= 'gsearch'
    AND ws.utm_campaign = 'nonbrand'
GROUP BY 1),

t2 AS
(
    SELECT
        t1.website_session_id AS session_id,
        wp.pageview_url AS landing_page
    FROM t1 LEFT JOIN website_pageviews wp
        ON t1.min_pageview_id = wp.website_pageview_id
    WHERE wp.pageview_url IN ('/home', '/lander-1')
),

t3 AS 
(
    SELECT
        t2.session_id AS bounced_session_id,
        t2.landing_page,
        COUNT(wp.website_pageview_id) AS count_of_pages_viewed
    FROM t2 LEFT JOIN website_pageviews wp
        ON t2.session_id = wp.website_session_id
    GROUP BY 1,2
    HAVING count_of_pages_viewed = 1
)

SELECT 
    t2.landing_page,
    COUNT(DISTINCT t2.session_id) AS total_sessions,
    COUNT(DISTINCT t3.bounced_session_id) AS bounced_sessions,
    COUNT(DISTINCT t3.bounced_session_id)/COUNT (DISTINCT t2.session_id) AS bounce_rate
FROM t2 LEFT JOIN t3
    ON t2.session_id = t3.bounced_session_id
GROUP BY t2.landing_page;

-- 2.2 Analyzing Sales Lift
SELECT
    MIN(website_pageview_id) AS first_test_pv
FROM website_pageviews
WHERE pageview_url ='/lander-1';

WITH t1 AS
(
    SELECT
        p.website_session_id AS session_id,
        MIN(p.website_pageview_id) AS min_pageview_id
    FROM website_pageviews p JOIN website_sessions s
        ON p.website_session_id = s.website_session_id
        AND s.created_at < '2012-07-28'
        AND p.website_pageview_id >= 23504
        AND utm_source = 'gsearch'
        AND utm_campaign = 'nonbrand'
    GROUP BY 1),

t2 AS
(
    SELECT
        session_id AS session_id,
        w.pageview_url AS landing_page
    FROM t1 LEFT JOIN website_pageviews w
        ON t1.min_pageview_id = w.website_pageview_id
        WHERE w.pageview_url IN ('/home', '/lander-1')
),

t3 AS
(
    SELECT t2.session_id, t2.landing_page, o.order_id
    FROM t2 LEFT JOIN orders o ON t2.session_id = o.website_session_id
)

SELECT 
    landing_page,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT (DISTINCT order_id) AS orders,
    COUNT(DISTINCT order_id)/COUNT (DISTINCT session_id) AS conv_rate
FROM t3 GROUP BY 1;


SELECT MAX(s.website_session_id) AS most_recent_gsearch_nonbrand_home_pageview
FROM website_sessions s LEFT JOIN website_pageviews w
    ON s.website_session_id = w.website_session_id
WHERE s.utm_source= 'gsearch'
    AND s.utm_campaign='nonbrand'
    AND w.pageview_url ='/home'
    AND s.created_at < '2012-11-27'; 

SELECT COUNT(website_session_id) AS sessions_since_test
FROM website_sessions
WHERE created_at <'2012-11-27'
    AND website_session_id > 17145
    AND utm_source= 'gsearch'
    AND utm_campaign = 'nonbrand' ;

-- 3. Analyzing & Testing Conversion Funnels
-- 3.1 Building & Analyzing Conversion Funnels
WITH t1 AS
(
    SELECT
        s.website_session_id AS session_id,
        w.pageview_url AS pageview_url,
        CASE WHEN pageview_url='/products' THEN 1 ELSE 0 END AS products_page,
        CASE WHEN pageview_url='/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS mrfuzzy_page,
        CASE WHEN pageview_url ='/cart' THEN 1 ELSE 0 END AS cart_page,
        CASE WHEN pageview_url='/shipping' THEN 1 ELSE 0 END AS shiping_page,
        CASE WHEN pageview_url ='/billing' THEN 1 ELSE 0 END AS billing_page,
        CASE WHEN pageview_url='/thank-you-for-your-order' THEN 1 ELSE 0 END AS thankyou_page
    FROM website_sessions s LEFT JOIN website_pageviews w
        ON s.website_session_id = w.website_session_id
    WHERE s.utm_source= 'gsearch'
        AND s.utm_campaign='nonbrand'
        AND s.created_at> '2012-08-05'
        AND s.created_at < '2012-09-05'
    ORDER BY s.website_session_id, w.created_at
),

t2 AS
(
    SELECT
        session_id,
        MAX(products_page) AS product_made_it,
        MAX(mrfuzzy_page) AS mrfuzzy_made_it,
        MAX(cart_page) AS cart_made_it,
        MAX(shiping_page) AS shipping_made_it,
        MAX(billing_page) AS billing_made_it,
        MAX(thankyou_page) AS thankyou_made_it
    FROM t1
    GROUP BY 1
) 

SELECT
    COUNT(DISTINCT CASE WHEN product_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT (DISTINCT session_id) AS lander_click_rt,
    COUNT(DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN product_made_it = 1 THEN session_id ELSE NULL END) AS products_click_rt,
    COUNT(DISTINCT CASE WHEN cart_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN session_id ELSE NULL END) AS mrfuzzy_click_rt,
    COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN cart_made_it = 1 THEN session_id ELSE NULL END) AS cart_click_rt,
    COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN shipping_made_it = 1 THEN session_id ELSE NULL END) AS shipping_click_rt,
    COUNT(DISTINCT CASE WHEN thankyou_made_it = 1 THEN session_id ELSE NULL END)
        /COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN session_id ELSE NULL END) AS billing_click_rt
FROM t2;

-- 3.2 Analyzing Conversion Funnel Tests
SELECT MIN(website_pageview_id) AS first_billing2_pv_id
FROM website_pageviews
WHERE pageview_url = '/billing-2';

WITH t1 AS
(
    SELECT
        w.website_session_id AS session_id,
        w.pageview_url AS billing_version_seen,
        o.order_id
    FROM website_pageviews w LEFT JOIN orders o
        ON w.website_session_id = o.website_session_id
    WHERE w.website_pageview_id >= 53550
        AND w.created_at < '2012-11-10'
        AND w.pageview_url IN ('/billing', '/billing-2')
)

SELECT 
    billing_version_seen,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT order_id) AS Orders,
    COUNT(DISTINCT order_id)/COUNT(DISTINCT session_id) AS billing_to_order_rt
FROM t1
GROUP BY 1;

-----------------------------BUSINESS PATTERNS AND SEASONALITY----------------------------------------

-- 1. Analyzing Seasonality
SELECT
    YEAR (s.created_at) AS yr,
    MONTH(s.created_at) AS mo,
    COUNT (DISTINCT s.website_session_id) AS sessions,
    COUNT (DISTINCT o.order_id) AS orders
FROM website_sessions s LEFT JOIN orders o
    ON s.website_session_id = o.website_session_id
WHERE s.created_at <'2013-01-01'
GROUP BY 1,2;

-- 2. Analyzing Business Patterns
WITH daily_hourly_sessions AS
(
    SELECT
        DATE (created_at) AS created_date,
        WEEKDAY (created_at) AS wkday,
        HOUR (created_at) AS hr,
        COUNT(DISTINCT website_session_id) AS sessions
    FROM website_sessions
    WHERE created_at BETWEEN '2012-09-15' AND '2012-11-15'
    GROUP BY 1,2,3
)

SELECT
    hr,
    ROUND (AVG (CASE WHEN wkday = 0 THEN sessions ELSE NULL END),1) AS mon,
    ROUND (AVG (CASE WHEN wkday = 1 THEN sessions ELSE NULL END),1) AS tue,
    ROUND (AVG (CASE WHEN wkday = 2 THEN sessions ELSE NULL END),1) AS wed,
    ROUND (AVG (CASE WHEN wkday = 3 THEN sessions ELSE NULL END),1) AS thu,
    ROUND (AVG (CASE WHEN wkday = 4 THEN sessions ELSE NULL END),1) AS fri,
    ROUND (AVG (CASE WHEN wkday = 5 THEN sessions ELSE NULL END),1) AS sat,
    ROUND (AVG (CASE WHEN wkday = 6 THEN sessions ELSE NULL END),1) AS sun
FROM daily_hourly_sessions
GROUP BY hr
ORDER BY hr;

-----------------------------------------PRODUCT ANALYSIS----------------------------------------------------

-- 1. Impact Of New Product Launch
SELECT
    YEAR (s.created_at) AS yr,
    MONTH(s.created_at) AS mo,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT(DISTINCT o.order_id)/COUNT (DISTINCT s.website_session_id) AS conv_rate,
    SUM(o.price_usd)/COUNT(DISTINCT s.website_session_id) AS revenue_per_session,
    COUNT(DISTINCT CASE WHEN primary_product_id = 1 THEN o.order_id ELSE NULL END) AS product_one_orders,
    COUNT(DISTINCT CASE WHEN primary_product_id = 2 THEN o.order_id ELSE NULL END) AS product_two_orders
FROM website_sessions s
    LEFT JOIN orders o
        ON s.website_session_id = o.website_session_id
WHERE s.created_at < '2013-04-05'
    AND s.created_at> '2012-04-01'
GROUP BY 1,2;

-- 2. Product Level Website Pathing
SELECT
    wp.pageview_url,
    COUNT (DISTINCT wp.website_session_id) AS sessions,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT order_id)/COUNT(DISTINCT wp.website_session_id) AS viewed_to_products_order_rate
FROM website_pageviews wp LEFT JOIN orders o
    ON wp.website_session_id = o.website_session_id
WHERE wp.created_at BETWEEN '2013-02-01' AND '2013-03-01'
    AND wp.pageview_url IN ('/the-original-mr-fuzzy', '/the-forever-love-bear')
GROUP BY 1;

-- 3. Product Portfolio Expansion
WITH t1 AS
(
    SELECT
        website_session_id,
        website_pageview_id,
        created_at AS saw_product_page_at
    FROM website_pageviews
    WHERE pageview_url = '/products'
) 
SELECT
    YEAR (saw_product_page_at) AS yr,
    MONTH(saw_product_page_at) AS mo,
    COUNT (DISTINCT t1.website_session_id) AS sessions_to_product_page,
    COUNT(DISTINCT pv.website_session_id) AS clicked_to_next_page,
    COUNT (DISTINCT pv.website_session_id)/COUNT (DISTINCT t1.website_session_id) AS clickthrough_rate,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT (DISTINCT o.order_id)/COUNT(DISTINCT t1.website_session_id) AS products_to_order_rate
FROM t1
    LEFT JOIN website_pageviews pv
        ON t1.website_session_id = pv.website_session_id
        AND pv.website_pageview_id > t1.website_pageview_id
    LEFT JOIN orders o
        ON o.website_session_id = t1.website_session_id
GROUP BY 1,2;

-- 4. Cross-Sell And Product Portfolio Analysis
WITH t1 AS
(
    SELECT
        order_id,
        primary_product_id,
        created_at AS ordered_at
    FROM orders
    WHERE created_at > '2014-12-05' 
),

t2 AS
(
    SELECT
        t1.*,
        oi.product_id AS cross_sell_product_id
    FROM t1 LEFT JOIN order_items oi
        ON t1.order_id = oi.order_id
        AND oi.is_primary_item = 0
) 

SELECT primary_product_id, 
    COUNT (DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN cross_sell_product_id = 1 THEN order_id ELSE NULL END) AS _xsold_p1,
    COUNT(DISTINCT CASE WHEN cross_sell_product_id = 2 THEN order_id ELSE NULL END) AS _xsold_p2,
    COUNT (DISTINCT CASE WHEN cross_sell_product_id = 3 THEN order_id ELSE NULL END) AS _xsold_p3,
    COUNT(DISTINCT CASE WHEN cross_sell_product_id = 4 THEN order_id ELSE NULL END) AS _xsold_p4,
    COUNT (DISTINCT CASE WHEN cross_sell_product_id = 1 THEN order_id ELSE NULL END)/COUNT(DISTINCT order_id) AS p1_xsell_rt,
    COUNT (DISTINCT CASE WHEN cross_sell_product_id = 2 THEN order_id ELSE NULL END)/COUNT(DISTINCT order_id) AS p2_xsell_rt,
    COUNT (DISTINCT CASE WHEN cross_sell_product_id = 3 THEN order_id ELSE NULL END)/COUNT(DISTINCT order_id) AS p3_xsell_rt,
    COUNT (DISTINCT CASE WHEN cross_sell_product_id = 4 THEN order_id ELSE NULL END)/COUNT(DISTINCT order_id) AS p4_xsell_rt
FROM t2
GROUP BY 1;

-- 5. Product Refund Analysis
SELECT 
    YEAR(i.created_at) AS yr,
    MONTH(i.created_at) AS mo, 
    COUNT(DISTINCT CASE WHEN product_id = 1 THEN i.order_item_id ELSE NULL END) AS p1_orders,
    COUNT(DISTINCT CASE WHEN product_id = 1 THEN r.order_item_refund_id ELSE NULL END)
        /COUNT(DISTINCT CASE WHEN product_id = 1 THEN i.order_item_id ELSE NULL END) AS p1_refund_rt,
    COUNT(DISTINCT CASE WHEN product_id = 2 THEN i.order_item_id ELSE NULL END) AS p2_orders,
    COUNT(DISTINCT CASE WHEN product_id = 2 THEN r.order_item_refund_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN product_id = 2 THEN i.order_item_id ELSE NULL END) AS p2_refund_rt,
    COUNT(DISTINCT CASE WHEN product_id = 3 THEN i.order_item_id ELSE NULL END) AS p3_orders,
    COUNT(DISTINCT CASE WHEN product_id = 3 THEN r.order_item_refund_id ELSE NULL END)
        /COUNT(DISTINCT CASE WHEN product_id = 3 THEN i.order_item_id ELSE NULL END) AS p3_refund_rt,
    COUNT(DISTINCT CASE WHEN product_id = 4 THEN i.order_item_id ELSE NULL END) AS p4_orders,
    COUNT(DISTINCT CASE WHEN product_id = 4 THEN r.order_item_refund_id ELSE NULL END)
        /COUNT (DISTINCT CASE WHEN product_id = 4 THEN i.order_item_id ELSE NULL END) AS p4_refund_rt
FROM order_items i
    LEFT JOIN order_item_refunds r 
        ON i.order_item_id = r.order_item_id
WHERE i.created_at < '2014-10-15'
GROUP BY 1,2;

------------------------------------------USER ANALYSIS----------------------------------------------------

-- 1. Identifying Repeat Visitors
WITH t1 AS
(
    SELECT user_id, website_session_id
    FROM website_sessions
    WHERE created_at < '2014-11-01'
    AND created_at >= '2014-01-01'
    AND is_repeat_session = 0
),

t2 AS
(
    SELECT t1.user_id,
    t1.website_session_id AS new_session_id,
    ws.website_session_id AS repeat_session_id
    FROM t1 LEFT JOIN website_sessions ws
    ON t1.user_id = ws.user_id
    AND ws.is_repeat_session = 1 
    AND ws.website_session_id > t1.website_session_id
    AND ws.created_at < '2014-11-01'
    AND ws.created_at >= '2014-01-01'
),

t3 AS
(
    SELECT user_id,
    COUNT(DISTINCT new_session_id) AS new_sessions,
    COUNT(DISTINCT repeat_session_id) AS repeat_sessions
FROM t2
GROUP BY 1 
ORDER BY 3 DESC
)
SELECT repeat_sessions, COUNT (DISTINCT user_id) AS users
FROM t3 GROUP BY 1;

-- 2. Analyzing Time To Repeat
WITH t1 AS
(
    SELECT user_id,
        website_session_id,
        created_at
    FROM website_sessions
    WHERE created_at < '2014-11-03' 
    AND created_at >= '2014-01-01' 
    AND is_repeat_session = 0
), 

t2 AS
(
    SELECT t1.user_id,
        t1.website_session_id AS new_session_id,
        t1.created_at AS new_session_created_at,
        s.website_session_id AS repeat_session_id,
        s.created_at AS repeat_session_created_at
    FROM t1 LEFT JOIN website_sessions s
        ON t1.user_id = s.user_id
        AND s.is_repeat_session = 1 
        AND s.website_session_id > t1.website_session_id
        AND s.created_at < '2014-11-03' 
        AND s.created_at >= '2014-01-01'
), 

t3 AS
(
    SELECT user_id, new_session_id, new_session_created_at,
        MIN(repeat_session_id) AS second_session_id,
        MIN(repeat_session_created_at) AS second_session_created_at
    FROM t2
    WHERE repeat_session_id IS NOT NULL
    GROUP BY 1,2,3
),

t4 AS
(
    SELECT user_id,
    DATEDIFF(second_session_created_at, new_session_created_at) AS days_first_to_second_session
    FROM t3
)

SELECT AVG(days_first_to_second_session) AS avg_days_first_to_second,
MIN(days_first_to_second_session) AS min_days_first_to_second,
MAX(days_first_to_second_session) AS max_days_first_to_second
FROM t4;

-- 3. Analyzing Repeat Channel
SELECT utm_source, utm_campaign,http_referer,
    COUNT (CASE WHEN is_repeat_session = 0 THEN website_session_id ELSE NULL END) AS new_sessions,
    COUNT (CASE WHEN is_repeat_session = 1 THEN website_session_id ELSE NULL END) AS repeat_sessions
FROM website_sessions
WHERE created_at < '2014-11-05' 
AND created_at >= '2014-01-01'
GROUP BY 1,2,3
ORDER BY 5 DESC;

-- SImplified Channel Grouping
SELECT
    CASE WHEN utm_source IS NULL AND http_referer IN ('https://www.gsearch.com', 'https://www.bsearch.com') THEN 'organic_search'
        WHEN utm_campaign = 'nonbrand' THEN 'paid_nonbrand'
        WHEN utm_campaign = 'brand' THEN 'paid_brand'
        WHEN utm_source IS NULL AND http_referer IS NULL THEN 'direct_type_in'
        WHEN utm_source= 'socialbook' THEN 'paid_social'
    END AS channel_group,
    COUNT (CASE WHEN is_repeat_session = 0 THEN website_session_id ELSE NULL END) AS new_sessions,
    COUNT (CASE WHEN is_repeat_session = 1 THEN website_session_id ELSE NULL END) AS repeat_sessions
FROM website_sessions
WHERE created_at < '2014-11-05' AND created_at >= '2014-01-01'
GROUP BY 1
ORDER BY repeat_sessions DESC;

-- 4. Analyzing New And Repeat Conversion Rates
SELECT
    is_repeat_session,
    COUNT (DISTINCT s.website_session_id) AS sessions,
    COUNT(DISTINCT o.order_id)/COUNT (DISTINCT s.website_session_id) AS conv_rate,
    SUM(price_usd)/COUNT(DISTINCT s.website_session_id) AS rev_per_session
FROM website_sessions s LEFT JOIN orders o
ON s.website_session_id = o.website_session_id
WHERE s.created_at < '2014-11-08'
AND s.created_at >= '2014-01-01'
GROUP BY 1;