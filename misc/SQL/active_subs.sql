USE Akshay;

-- Create the subscriptions table
CREATE TABLE customer_subscriptions (
    customer_id INT,
    subscription_id VARCHAR(10),
    plan_type VARCHAR(20),
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (customer_id, subscription_id)
);

-- Insert the subscription data
INSERT INTO customer_subscriptions (customer_id, subscription_id, plan_type, start_date, end_date) VALUES
(101, 'S1', 'Basic', '2023-01-01', '2023-01-31'),
(101, 'S2', 'Premium', '2023-01-20', '2023-02-20'),
(101, 'S3', 'Basic', '2023-02-01', '2023-03-31'),
(101, 'S4', 'Basic', '2023-03-20', '2023-04-20'),
(102, 'S5', 'Basic', '2023-02-01', '2023-02-15'),
(102, 'S6', 'Basic', '2023-02-16', '2023-02-28'),
(102, 'S7', 'Premium', '2023-03-16', '2023-04-16'),
(103, 'S8', 'Premium', '2023-01-10', '2023-03-10'),
(103, 'S9', 'Premium', '2023-02-01', '2023-02-28'),
(103, 'S10', 'Basic', '2023-03-01', '2023-03-30');

----------------------------------------------

with next_start_date as (
    select
        customer_id as cid,
        start_date,
        end_date,
        lead(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) as next_start_date
    from customer_subscriptions
),
overlap_check as (
    select
        cid,
        start_date,
        end_date,
        next_start_date,
        CASE 
            WHEN COALESCE(next_start_date, '1900-01-01') > end_date THEN 0
            ELSE 1
        END AS overlap_flag
    from next_start_date
),
total_active_subs_day_for_overlap as (
    select 
        cid,
        DATEDIFF(max_dt, min_dt) as active_days
    from (
        select 
            cid,
            max(end_Date) as max_dt,
            min(start_date) as min_dt
        from overlap_check
        where overlap_flag = 1
        group by 1
    ) t
),
total_active_subs_days_for_non_overlap as (
    select
        cid,
        DATEDIFF(end_date,start_date) as active_days
    from overlap_check
    where overlap_flag = 0
)
select 
    t1.cid,
    t1.active_days + t2.active_days as active_days
from total_active_subs_day_for_overlap t1
join total_active_subs_days_for_non_overlap t2 on t1.cid = t2.cid;


------------------------------Correct solution:
WITH ordered_subs AS (
    -- Add previous end_date for proper overlap detection
    SELECT 
        customer_id AS cid,
        subscription_id,
        start_date, 
        end_date,
        LAG(end_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS prev_end
    FROM customer_subscriptions
),

overlap_check AS (
    SELECT 
        cid,
        start_date,
        end_date,
        CASE 
            WHEN prev_end IS NULL OR prev_end < start_date THEN 0  -- No overlap with previous
            ELSE 1  -- Overlaps with previous
        END AS overlap_flag,
        CASE 
            WHEN prev_end IS NULL OR prev_end < start_date THEN DATEDIFF(end_date, start_date) + 1
            ELSE NULL  -- Handle in overlap CTE
        END AS non_overlap_days
    FROM ordered_subs
),

non_overlap_total AS (
    -- Sum isolated subscriptions (handles cust 103 S10)
    SELECT cid, SUM(non_overlap_days) AS days FROM overlap_check 
    WHERE overlap_flag = 0 GROUP BY cid
),

overlap_groups AS (
    -- Identify continuous overlap chains and their coverage
    SELECT 
        cid,
        start_date,
        end_date,
        prev_end,
        SUM(CASE WHEN overlap_flag = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY cid ORDER BY start_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_id
    FROM overlap_check
),

overlap_coverage AS (
    -- Calculate unique coverage per overlap group (min start to max end)
    SELECT 
        cid,
        group_id,
        MIN(start_date) AS group_start,
        MAX(end_date) AS group_end,
        DATEDIFF(MAX(end_date), MIN(start_date)) + 1 AS group_days
    FROM overlap_groups 
    WHERE overlap_flag = 1
    GROUP BY cid, group_id
),

overlap_total AS (
    SELECT cid, SUM(group_days) AS days FROM overlap_coverage GROUP BY cid
)

-- Combine: overlaps + non-overlaps (with FULL OUTER JOIN equivalent)
SELECT 
    COALESCE(ot.cid, nt.cid) AS cid,
    COALESCE(ot.days, 0) + COALESCE(nt.days, 0) AS active_days
FROM overlap_total ot
FULL OUTER JOIN non_overlap_total nt ON ot.cid = nt.cid
ORDER BY cid;



