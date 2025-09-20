WITH 
monthly_revenue AS (
    SELECT
        date_trunc('month', gp.payment_date)::date AS payment_month,
        gp.user_id,
        gp.game_name,
        SUM(gp.revenue_amount_usd) AS total_revenue
    FROM project.games_payments gp
    GROUP BY 1, 2, 3
),
revenue_lag_lead_months AS (
    SELECT
        mr.*,
        (mr.payment_month - INTERVAL '1 month')::date AS previous_calendar_month,
        (mr.payment_month + INTERVAL '1 month')::date AS next_calendar_month,
        LAG(mr.payment_month) OVER (PARTITION BY mr.user_id, mr.game_name ORDER BY mr.payment_month) AS previous_paid_month,
        LAG(mr.total_revenue) OVER (PARTITION BY mr.user_id, mr.game_name ORDER BY mr.payment_month) AS previous_paid_month_revenue,
        LEAD(mr.payment_month) OVER (PARTITION BY mr.user_id, mr.game_name ORDER BY mr.payment_month) AS next_paid_month
    FROM monthly_revenue mr
),
revenue_metrics AS (
    SELECT
        rll.payment_month,
        rll.user_id,
        rll.game_name,
        rll.total_revenue AS mrr,
        CASE WHEN rll.previous_paid_month IS NULL THEN rll.total_revenue ELSE 0 END AS new_mrr,
        CASE 
            WHEN rll.previous_paid_month = rll.previous_calendar_month 
                 AND rll.total_revenue > rll.previous_paid_month_revenue
            THEN rll.total_revenue - rll.previous_paid_month_revenue
            ELSE 0
        END AS expansion_mrr,
        CASE 
            WHEN rll.previous_paid_month = rll.previous_calendar_month 
                 AND rll.total_revenue < rll.previous_paid_month_revenue
            THEN rll.total_revenue - rll.previous_paid_month_revenue
            ELSE 0
        END AS contraction_mrr,
        CASE 
            WHEN rll.previous_paid_month IS NOT NULL 
                 AND rll.previous_paid_month <> rll.previous_calendar_month
            THEN rll.total_revenue
            ELSE 0
        END AS back_from_churn_mrr,
        CASE 
            WHEN rll.next_paid_month IS NULL 
                 OR rll.next_paid_month <> rll.next_calendar_month
            THEN rll.total_revenue
            ELSE 0
        END AS churned_revenue,
        CASE 
            WHEN rll.next_paid_month IS NULL 
                 OR rll.next_paid_month <> rll.next_calendar_month
            THEN rll.next_calendar_month
            ELSE NULL
        END AS churn_month,
        CASE WHEN rll.total_revenue > 0 THEN 1 ELSE 0 END AS paid_users,
        CASE WHEN rll.previous_paid_month IS NULL THEN 1 ELSE 0 END AS new_paid_users,
        CASE WHEN rll.next_paid_month IS NULL OR rll.next_paid_month <> rll.next_calendar_month THEN 1 ELSE 0 END AS churned_users
    FROM revenue_lag_lead_months rll
)
SELECT
    rm.payment_month,
    rm.churn_month,
    rm.user_id,
    rm.mrr,
    rm.new_mrr,
    rm.expansion_mrr,
    rm.contraction_mrr,
    rm.back_from_churn_mrr,
    rm.churned_revenue,
    rm.paid_users,
    rm.new_paid_users,
    rm.churned_users,
    gpu.language,
    gpu.has_older_device_model,
    gpu.age,
    gpu.game_name
FROM revenue_metrics rm
LEFT JOIN project.games_paid_users gpu ON rm.user_id = gpu.user_id
ORDER BY rm.payment_month, rm.user_id;
