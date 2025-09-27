/*
Enter your query below.
Please append a semicolon ";" at the end of the query
*/
with month_year_data as (
    select
        *,
        DATE_FORMAT(dt,'%Y-%m') as month_year
    from orders
    
),

monthly_sales as (
    select
        coupon_id,
        month_year,
        sum(total_amount) as monthly_sales
    from month_year_data
    group by 1,2
    
),

coupon_rank as (
    select
        month_year,
        coupon_id,
        DENSE_RANK() OVER (PARTITION BY month_year ORDER BY monthly_sales DESC) as usage_rank
    from monthly_sales
        
),

mom_data as (
    select
        *,
        CASE WHEN prev_m_sales is null
             THEN 'N/A'
             ELSE ((monthly_sales-prev_m_sales)/prev_m_sales)*100  END as mom_growth
    FROM (
        select 
            *,
            lag(monthly_sales) OVER (PARTITION BY month_year ORDER BY month_year) as prev_m_sales
        from monthly_sales
    ) ps
),

coupon_avg_sale as (
    select
        coupon_id,
        month_year,
        avg(monthly_sales) OVER (ORDER BY month_year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_last_3_months
    from monthly_sales
)

select
    c.coupon_code,
    ms.month_year,
    ms.monthly_sales,
    round(mom.mom_growth,2) as mom_growth,
    cr.usage_rank,
    round(av.avg_last_3_months,2) as avg_last_3_months

FROM monthly_sales ms 
join mom_data mom on ms.coupon_id = mom.coupon_id
                    and ms.month_year = mom.month_year
join coupon_rank cr on ms.coupon_id = cr.coupon_id
                    and ms.month_year = cr.month_year
join coupon_avg_sale av on ms.coupon_id = av.coupon_id
                    and ms.month_year = av.month_year
join coupons c on ms.coupon_id = c.id

order by 2,5;


