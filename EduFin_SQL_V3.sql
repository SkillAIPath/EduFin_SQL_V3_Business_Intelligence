-- =====================================================
-- VERSION 3 ENHANCED: Strategic Business Intelligence
-- =====================================================

-- CHALLENGE: Market Expansion Decision Engine
-- Business Context: ₹200Cr Series B funding allocation
-- Corporate Reality: Board-level analysis requiring multiple data sources

-- Multi-dimensional Market Opportunity Analysis
WITH market_penetration_analysis AS (
    SELECT 
        gd.state,
        gd.city,
        gd.tier_classification,
        gd.population_18_35,
        gd.higher_education_enrollment,
        gd.average_household_income,
        gd.unemployment_rate,
        
        -- Current EduFin presence
        COUNT(DISTINCT c.customer_id) as our_customers,
        COUNT(DISTINCT l.loan_id) as our_loans,
        SUM(l.loan_amount) as our_portfolio_value,
        AVG(l.loan_amount) as our_avg_loan_size,
        
        -- Performance metrics
        ROUND(AVG(CASE WHEN l.loan_status = 'Defaulted' THEN 1.0 ELSE 0.0 END) * 100, 2) as our_default_rate,
        AVG(DATEDIFF(DAY, l.application_date, l.disbursement_date)) as our_avg_processing_time,
        
        -- Market opportunity calculation
        ROUND(gd.higher_education_enrollment * 0.35,0) as addressable_market_size,
        ROUND(gd.average_household_income * 0.6,0) as estimated_avg_loan_demand,
        
        -- Competitive intelligence (estimated)
        CASE 
            WHEN gd.tier_classification = 'Tier1' THEN ROUND(gd.higher_education_enrollment * 0.35 * 0.45,0)
            WHEN gd.tier_classification = 'Tier2' THEN ROUND(gd.higher_education_enrollment * 0.35 * 0.25,0)
            ELSE ROUND(gd.higher_education_enrollment * 0.35 * 0.10,0)
        END as estimated_market_penetration,
        
        -- Economic indicators
        ei.gdp_growth_rate,
        ei.inflation_rate,
        ei.employment_rate,
        ei.education_expenditure_percentage
        
    FROM geographic_demographics gd
    LEFT JOIN sample_customers c ON gd.city = c.current_city
    LEFT JOIN sample_loans l ON c.customer_id = l.customer_id
    LEFT JOIN economic_indicators ei ON gd.state = ei.state 
        AND YEAR(l.disbursement_date) = ei.year
    WHERE gd.higher_education_enrollment >= 5000
    GROUP BY gd.state, gd.city, gd.tier_classification, gd.population_18_35,
             gd.higher_education_enrollment, gd.average_household_income, gd.unemployment_rate,
             ei.gdp_growth_rate, ei.inflation_rate, ei.employment_rate, ei.education_expenditure_percentage
),
investment_scoring_engine AS (
    SELECT *,
        -- Market attractiveness score (0-100)
        (
            -- Market size component (40%)
            CASE 
                WHEN addressable_market_size >= 50000 THEN 40
                WHEN addressable_market_size >= 20000 THEN 32
                WHEN addressable_market_size >= 10000 THEN 24
                WHEN addressable_market_size >= 5000 THEN 16
                ELSE 8
            END +
            
            -- Economic health component (30%)
            CASE 
                WHEN gdp_growth_rate >= 8 AND unemployment_rate <= 5 THEN 30
                WHEN gdp_growth_rate >= 6 AND unemployment_rate <= 8 THEN 24
                WHEN gdp_growth_rate >= 4 AND unemployment_rate <= 12 THEN 18
                ELSE 12
            END +
            
            -- Competitive gap component (20%)
            CASE 
                WHEN ISNULL(our_customers, 0) = 0 THEN 20  -- Greenfield opportunity
                WHEN our_customers < (estimated_market_penetration * 0.05) THEN 16
                WHEN our_customers < (estimated_market_penetration * 0.15) THEN 12
                ELSE 8
            END +
            
            -- Risk assessment component (10%)
            CASE 
                WHEN ISNULL(our_default_rate, 8) <= 6 THEN 10
                WHEN ISNULL(our_default_rate, 8) <= 10 THEN 8
                WHEN ISNULL(our_default_rate, 8) <= 15 THEN 6
                ELSE 4
            END
        ) as market_attractiveness_score,
        
        -- Investment requirement estimation
        CASE 
            WHEN tier_classification = 'Tier1' THEN 
                CASE WHEN our_customers = 0 THEN 8000000 ELSE 3000000 END
            WHEN tier_classification = 'Tier2' THEN 
                CASE WHEN our_customers = 0 THEN 5000000 ELSE 2000000 END
            ELSE 
                CASE WHEN our_customers = 0 THEN 2000000 ELSE 1000000 END
        END as estimated_investment_required,
        
        -- Revenue projection (5-year NPV)
        ROUND(
            (addressable_market_size * estimated_avg_loan_demand * 0.12 * 
             POWER(1 + ISNULL(gdp_growth_rate, 6)/100, 5)) / 
            POWER(1.10, 5),0  -- 10% discount rate
        ) as five_year_npv,
        
        -- Market share potential
        CASE 
            WHEN estimated_market_penetration > 0 AND our_customers > 0
            THEN ROUND(our_customers * 100.0 / estimated_market_penetration, 2)
            ELSE 0
        END as current_market_share_percent
        
    FROM market_penetration_analysis
),
strategic_prioritization AS (
    SELECT *,
        -- ROI calculation
        CASE 
            WHEN estimated_investment_required > 0 
            THEN ROUND(five_year_npv / estimated_investment_required, 2)
            ELSE 0
        END as roi_multiple,
        
        -- Strategic fit assessment
        CASE 
            WHEN market_attractiveness_score >= 80 AND five_year_npv / estimated_investment_required >= 5
            THEN 'TIER 1: Immediate expansion - High priority'
            WHEN market_attractiveness_score >= 70 AND five_year_npv / estimated_investment_required >= 3
            THEN 'TIER 2: Strategic expansion - Medium priority'
            WHEN market_attractiveness_score >= 60 AND five_year_npv / estimated_investment_required >= 2
            THEN 'TIER 3: Future consideration - Long term'
            ELSE 'TIER 4: Avoid - Low priority'
        END as expansion_priority,
        
        -- Implementation complexity
        CASE 
            WHEN our_customers > 0 THEN 'LOW: Build on existing presence'
            WHEN tier_classification = 'Tier1' THEN 'HIGH: Complex regulatory setup'
            WHEN tier_classification = 'Tier2' THEN 'MEDIUM: Standard setup process'
            ELSE 'LOW: Quick deployment possible'
        END as implementation_complexity,
        
        -- Timeline estimation
        CASE 
            WHEN our_customers > 0 THEN 'Q1: Immediate scale-up'
            WHEN tier_classification = 'Tier1' THEN 'Q2-Q3: Extended setup'
            WHEN tier_classification = 'Tier2' THEN 'Q1-Q2: Standard timeline'
            ELSE 'Q1: Rapid deployment'
        END as implementation_timeline
        
    FROM investment_scoring_engine
)
-- Final Board Presentation Query
SELECT 
    expansion_priority,
    COUNT(*) as markets_in_tier,
    SUM(estimated_investment_required) as total_investment_needed,
    SUM(five_year_npv) as total_revenue_potential,
    AVG(roi_multiple) as avg_roi_multiple,
    AVG(market_attractiveness_score) as avg_attractiveness_score,
    
    -- Strategic recommendations
    CASE 
        WHEN expansion_priority LIKE 'TIER 1%' 
        THEN 'RECOMMENDED: Allocate 60% of ₹200Cr funding'
        WHEN expansion_priority LIKE 'TIER 2%' 
        THEN 'RECOMMENDED: Allocate 30% of ₹200Cr funding'
        WHEN expansion_priority LIKE 'TIER 3%' 
        THEN 'RECOMMENDED: Allocate 10% of ₹200Cr funding'
        ELSE 'NOT RECOMMENDED: Focus resources elsewhere'
    END as funding_recommendation,
    
    -- Market entry sequence
    STRING_AGG(city + ' (' + state + ')', ', ') WITHIN GROUP (ORDER BY market_attractiveness_score DESC) as recommended_entry_sequence
    
FROM strategic_prioritization
WHERE market_attractiveness_score >= 50  -- Minimum viable markets
GROUP BY expansion_priority
ORDER BY avg_roi_multiple DESC;

-- =====================================================
-- PERFORMANCE MONITORING QUERIES
-- =====================================================
DECLARE @start_time DATETIME = GETDATE();
-- Query Performance Tracker
WITH query_performance_log AS (
    SELECT 
        'Portfolio Dashboard' as query_name,
        GETDATE() as execution_time,
        @@ROWCOUNT as rows_processed,
        DATEDIFF(MILLISECOND, @start_time, GETDATE()) as execution_duration_ms
)
SELECT * FROM query_performance_log;

-- Data Quality Validation
SELECT 
    'Data Quality Check' as validation_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN loan_amount IS NULL THEN 1 END) as null_loan_amounts,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_ids,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(disbursement_date) as earliest_date,
    MAX(disbursement_date) as latest_date
FROM sample_loans;

-- Business Logic Validation
SELECT 
    'Business Rules Validation' as check_type,
    SUM(CASE WHEN loan_amount > c.annual_income * 3 THEN 1 ELSE 0 END) as high_risk_loans,
    AVG(CASE WHEN loan_status = 'Defaulted' THEN 1.0 ELSE 0.0 END) * 100 as actual_default_rate,
    COUNT(CASE WHEN emi_amount > loan_amount THEN 1 END) as invalid_emi_calculations
FROM sample_loans l
INNER JOIN sample_customers c ON l.customer_id = c.customer_id;