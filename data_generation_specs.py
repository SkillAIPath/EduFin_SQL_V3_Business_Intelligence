# =====================================================
# EduFin Credit Solutions - Industrial Data Generation
# 1M+ Record Dataset for Corporate SQL Training
# Tools: Databricks + dbldatagen + faker
# =====================================================

import dbldatagen as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from faker import Faker
import random
import numpy as np
from datetime import datetime, timedelta
import uuid

# Initialize Spark and Faker
spark = SparkSession.builder.appName("EduFin_DataGen").getOrCreate()
fake = Faker('en_IN')  # Indian locale for realistic names and addresses

# =====================================================
# BUSINESS RULES AND CONFIGURATION
# =====================================================

class EduFinDataConfig:
    """Configuration class for realistic business data generation"""
    
    # Dataset sizes
    TOTAL_CUSTOMERS = 100000
    TOTAL_INSTITUTIONS = 5000
    TOTAL_LOANS = 150000
    TOTAL_PAYMENTS = 800000
    TOTAL_DEFAULTS = 18000  # 12% of loans
    
    # Date ranges
    BUSINESS_START_DATE = datetime(2019, 1, 1)
    CURRENT_DATE = datetime(2024, 6, 30)
    
    # Geographic distribution (Indian cities)
    CITY_DISTRIBUTION = {
        # Tier 1 cities (15% of customers)
        'Mumbai': {'state': 'Maharashtra', 'tier': 'Tier1', 'weight': 0.04, 'avg_income': 450000, 'income_std': 200000},
        'Delhi': {'state': 'Delhi', 'tier': 'Tier1', 'weight': 0.035, 'avg_income': 420000, 'income_std': 180000},
        'Bangalore': {'state': 'Karnataka', 'tier': 'Tier1', 'weight': 0.035, 'avg_income': 480000, 'income_std': 220000},
        'Hyderabad': {'state': 'Telangana', 'tier': 'Tier1', 'weight': 0.025, 'avg_income': 380000, 'income_std': 170000},
        'Chennai': {'state': 'Tamil Nadu', 'tier': 'Tier1', 'weight': 0.03, 'avg_income': 400000, 'income_std': 180000},
        'Kolkata': {'state': 'West Bengal', 'tier': 'Tier1', 'weight': 0.025, 'avg_income': 350000, 'income_std': 160000},
        
        # Tier 2 cities (35% of customers)
        'Pune': {'state': 'Maharashtra', 'tier': 'Tier2', 'weight': 0.025, 'avg_income': 320000, 'income_std': 140000},
        'Ahmedabad': {'state': 'Gujarat', 'tier': 'Tier2', 'weight': 0.02, 'avg_income': 300000, 'income_std': 130000},
        'Jaipur': {'state': 'Rajasthan', 'tier': 'Tier2', 'weight': 0.018, 'avg_income': 280000, 'income_std': 120000},
        'Surat': {'state': 'Gujarat', 'tier': 'Tier2', 'weight': 0.015, 'avg_income': 290000, 'income_std': 125000},
        'Lucknow': {'state': 'Uttar Pradesh', 'tier': 'Tier2', 'weight': 0.018, 'avg_income': 260000, 'income_std': 110000},
        'Kanpur': {'state': 'Uttar Pradesh', 'tier': 'Tier2', 'weight': 0.015, 'avg_income': 250000, 'income_std': 100000},
        'Nagpur': {'state': 'Maharashtra', 'tier': 'Tier2', 'weight': 0.012, 'avg_income': 270000, 'income_std': 115000},
        'Indore': {'state': 'Madhya Pradesh', 'tier': 'Tier2', 'weight': 0.012, 'avg_income': 265000, 'income_std': 110000},
        'Thane': {'state': 'Maharashtra', 'tier': 'Tier2', 'weight': 0.015, 'avg_income': 340000, 'income_std': 150000},
        'Bhopal': {'state': 'Madhya Pradesh', 'tier': 'Tier2', 'weight': 0.01, 'avg_income': 255000, 'income_std': 105000},
        'Visakhapatnam': {'state': 'Andhra Pradesh', 'tier': 'Tier2', 'weight': 0.01, 'avg_income': 275000, 'income_std': 120000},
        'Vadodara': {'state': 'Gujarat', 'tier': 'Tier2', 'weight': 0.01, 'avg_income': 285000, 'income_std': 125000},
        'Patna': {'state': 'Bihar', 'tier': 'Tier2', 'weight': 0.012, 'avg_income': 220000, 'income_std': 90000},
        'Ludhiana': {'state': 'Punjab', 'tier': 'Tier2', 'weight': 0.008, 'avg_income': 295000, 'income_std': 130000},
        'Agra': {'state': 'Uttar Pradesh', 'tier': 'Tier2', 'weight': 0.01, 'avg_income': 240000, 'income_std': 95000},
        'Nashik': {'state': 'Maharashtra', 'tier': 'Tier2', 'weight': 0.008, 'avg_income': 265000, 'income_std': 115000},
        'Faridabad': {'state': 'Haryana', 'tier': 'Tier2', 'weight': 0.008, 'avg_income': 315000, 'income_std': 140000},
        'Meerut': {'state': 'Uttar Pradesh', 'tier': 'Tier2', 'weight': 0.008, 'avg_income': 245000, 'income_std': 100000},
        'Rajkot': {'state': 'Gujarat', 'tier': 'Tier2', 'weight': 0.007, 'avg_income': 275000, 'income_std': 120000},
        'Kalyan-Dombivali': {'state': 'Maharashtra', 'tier': 'Tier2', 'weight': 0.007, 'avg_income': 320000, 'income_std': 140000},
        
        # Tier 3 cities (25% of customers)
        'Kota': {'state': 'Rajasthan', 'tier': 'Tier3', 'weight': 0.015, 'avg_income': 180000, 'income_std': 70000},
        'Dehradun': {'state': 'Uttarakhand', 'tier': 'Tier3', 'weight': 0.008, 'avg_income': 240000, 'income_std': 95000},
        'Chandigarh': {'state': 'Chandigarh', 'tier': 'Tier3', 'weight': 0.006, 'avg_income': 350000, 'income_std': 150000},
        'Coimbatore': {'state': 'Tamil Nadu', 'tier': 'Tier3', 'weight': 0.008, 'avg_income': 260000, 'income_std': 110000},
        'Jodhpur': {'state': 'Rajasthan', 'tier': 'Tier3', 'weight': 0.006, 'avg_income': 210000, 'income_std': 85000},
        'Madurai': {'state': 'Tamil Nadu', 'tier': 'Tier3', 'weight': 0.007, 'avg_income': 230000, 'income_std': 90000},
        'Raipur': {'state': 'Chhattisgarh', 'tier': 'Tier3', 'weight': 0.006, 'avg_income': 220000, 'income_std': 85000},
        'Kochi': {'state': 'Kerala', 'tier': 'Tier3', 'weight': 0.007, 'avg_income': 280000, 'income_std': 120000},
        'Thiruvananthapuram': {'state': 'Kerala', 'tier': 'Tier3', 'weight': 0.006, 'avg_income': 270000, 'income_std': 115000},
        'Guwahati': {'state': 'Assam', 'tier': 'Tier3', 'weight': 0.005, 'avg_income': 250000, 'income_std': 100000},
        'Mangalore': {'state': 'Karnataka', 'tier': 'Tier3', 'weight': 0.004, 'avg_income': 270000, 'income_std': 110000},
        'Bhubaneswar': {'state': 'Odisha', 'tier': 'Tier3', 'weight': 0.005, 'avg_income': 240000, 'income_std': 95000},
        'Jabalpur': {'state': 'Madhya Pradesh', 'tier': 'Tier3', 'weight': 0.005, 'avg_income': 210000, 'income_std': 80000},
        
        # Small cities/towns (25% of customers) - distributed across remaining weight
        'Others': {'state': 'Various', 'tier': 'Tier3', 'weight': 0.15, 'avg_income': 150000, 'income_std': 60000}
    }
    
    # Educational institutions distribution
    INSTITUTION_TYPES = {
        'IIT': {'count': 23, 'avg_fee': 200000, 'default_rate': 0.02, 'ranking_range': (1, 50)},
        'NIT': {'count': 31, 'avg_fee': 150000, 'default_rate': 0.035, 'ranking_range': (15, 100)},
        'IIIT': {'count': 25, 'avg_fee': 180000, 'default_rate': 0.03, 'ranking_range': (20, 80)},
        'IIM': {'count': 20, 'avg_fee': 2000000, 'default_rate': 0.015, 'ranking_range': (1, 30)},
        'Medical College': {'count': 350, 'avg_fee': 800000, 'default_rate': 0.06, 'ranking_range': (1, 200)},
        'Private Engineering': {'count': 3000, 'avg_fee': 400000, 'default_rate': 0.12, 'ranking_range': (50, 1000)},
        'State University': {'count': 800, 'avg_fee': 80000, 'default_rate': 0.15, 'ranking_range': (100, 500)},
        'Private University': {'count': 500, 'avg_fee': 300000, 'default_rate': 0.10, 'ranking_range': (75, 300)},
        'Coaching Institute': {'count': 250, 'avg_fee': 120000, 'default_rate': 0.25, 'ranking_range': (1, 50)}
    }
    
    # Customer behavior profiles
    CUSTOMER_PROFILES = {
        'excellent': {'probability': 0.20, 'cibil_range': (750, 850), 'default_rate': 0.02, 'payment_delay_avg': 2},
        'good': {'probability': 0.45, 'cibil_range': (650, 749), 'default_rate': 0.06, 'payment_delay_avg': 5},
        'fair': {'probability': 0.25, 'cibil_range': (550, 649), 'default_rate': 0.15, 'payment_delay_avg': 12},
        'poor': {'probability': 0.10, 'cibil_range': (300, 549), 'default_rate': 0.35, 'payment_delay_avg': 25}
    }
    
    # Economic impact by year (multipliers)
    ECONOMIC_MULTIPLIERS = {
        2019: {'default_rate': 1.0, 'disbursement': 1.0, 'avg_income': 1.0},
        2020: {'default_rate': 1.8, 'disbursement': 0.6, 'avg_income': 0.9},  # COVID impact
        2021: {'default_rate': 1.5, 'disbursement': 0.8, 'avg_income': 0.95},  # Recovery
        2022: {'default_rate': 1.2, 'disbursement': 1.1, 'avg_income': 1.05},  # Normalization
        2023: {'default_rate': 1.0, 'disbursement': 1.3, 'avg_income': 1.08},  # Growth
        2024: {'default_rate': 0.9, 'disbursement': 1.4, 'avg_income': 1.12}   # Expansion
    }

# =====================================================
# INSTITUTIONS TABLE GENERATION
# =====================================================

def generate_institutions(spark, config):
    """Generate realistic educational institutions data"""
    
    institutions_data = []
    institution_id = 1
    
    for inst_type, type_config in config.INSTITUTION_TYPES.items():
        for i in range(type_config['count']):
            # Generate realistic institution names
            if inst_type == 'IIT':
                city = random.choice(['Mumbai', 'Delhi', 'Chennai', 'Kanpur', 'Kharagpur', 'Roorkee', 'Guwahati', 'Hyderabad'])
                name = f"Indian Institute of Technology {city}"
            elif inst_type == 'NIT':
                city = random.choice(['Trichy', 'Warangal', 'Surathkal', 'Calicut', 'Rourkela', 'Durgapur', 'Jaipur', 'Bhopal'])
                name = f"National Institute of Technology {city}"
            elif inst_type == 'IIM':
                city = random.choice(['Ahmedabad', 'Bangalore', 'Calcutta', 'Lucknow', 'Kozhikode', 'Indore', 'Shillong'])
                name = f"Indian Institute of Management {city}"
            elif inst_type == 'Medical College':
                city = random.choice(list(config.CITY_DISTRIBUTION.keys())[:30])
                name = f"{city} Medical College"
            elif inst_type == 'Coaching Institute':
                city = random.choice(['Kota', 'Delhi', 'Hyderabad', 'Pune', 'Mumbai'])
                name = f"{fake.company()} {city}"
            else:
                city = random.choice(list(config.CITY_DISTRIBUTION.keys())[:40])
                name = f"{city} {inst_type}"
            
            # Select city and get details
            city_info = config.CITY_DISTRIBUTION.get(city, config.CITY_DISTRIBUTION['Others'])
            
            institutions_data.append({
                'institution_id': institution_id,
                'institution_name': name,
                'institution_code': f"INST{institution_id:06d}",
                'institution_type': inst_type,
                'city': city,
                'state': city_info['state'],
                'tier_classification': city_info['tier'],
                'establishment_year': random.randint(1950, 2020),
                'nirf_ranking': random.randint(*type_config['ranking_range']) if type_config['ranking_range'][1] <= 1000 else None,
                'placement_percentage': min(100, max(30, random.normalvariate(85, 15))),
                'average_package': int(random.normalvariate(600000, 200000)),
                'partnership_start_date': fake.date_between(start_date='-5y', end_date='today'),
                'partnership_status': random.choices(['Active', 'Inactive', 'Under Review'], weights=[0.85, 0.10, 0.05])[0],
                'default_rate_percentage': max(0, random.normalvariate(type_config['default_rate'] * 100, 2)),
                'contact_person_name': fake.name(),
                'contact_email': fake.email(),
                'contact_phone': fake.phone_number()
            })
            institution_id += 1
    
    # Create DataFrame
    institutions_df = spark.createDataFrame(institutions_data)
    return institutions_df

# =====================================================
# CUSTOMERS TABLE GENERATION
# =====================================================

def generate_customers(spark, config):
    """Generate realistic customer data with proper distributions"""
    
    # Use dbldatagen for efficient large-scale generation
    customer_spec = (
        dg.DataGenerator(spark, name="customers", rows=config.TOTAL_CUSTOMERS)
        .withIdOutput()
        .withColumn("application_number", "string", template=r'APPL\d{8}', random=True)
        
        # Personal Information
        .withColumn("full_name", "string", values=[""] * config.TOTAL_CUSTOMERS, random=True)
        .withColumn("date_of_birth", "date", 
                   begin=datetime(1988, 1, 1), end=datetime(2006, 12, 31), 
                   random=True)
        .withColumn("gender", "string", values=["M", "F"], weights=[0.6, 0.4])
        .withColumn("mobile_number", "string", template=r'[6-9]\d{9}', random=True)
        .withColumn("email", "string", template=r'\w+@\w+\.com', random=True)
        .withColumn("pan_number", "string", template=r'[A-Z]{5}\d{4}[A-Z]', random=True)
        .withColumn("aadhar_number", "string", template=r'\d{12}', random=True)
        
        # Geographic Information - using weighted distribution
        .withColumn("current_city", "string", 
                   values=list(config.CITY_DISTRIBUTION.keys()),
                   weights=[config.CITY_DISTRIBUTION[city]['weight'] for city in config.CITY_DISTRIBUTION.keys()])
        
        # Financial Information - will be calculated based on city
        .withColumn("employment_type", "string",
                   values=["Student", "Part-time Employee", "Full-time Employee", "Self-employed", "Unemployed"],
                   weights=[0.45, 0.20, 0.25, 0.08, 0.02])
        
        # Customer Profile - determines credit behavior
        .withColumn("customer_profile", "string",
                   values=list(config.CUSTOMER_PROFILES.keys()),
                   weights=[config.CUSTOMER_PROFILES[profile]['probability'] 
                           for profile in config.CUSTOMER_PROFILES.keys()])
        
        .withColumn("registration_date", "date",
                   begin=config.BUSINESS_START_DATE, end=config.CURRENT_DATE,
                   random=True)
        .withColumn("kyc_status", "string",
                   values=["Verified", "Pending", "Rejected"],
                   weights=[0.85, 0.12, 0.03])
    )
    
    customers_df = customer_spec.build()
    
    # Add calculated fields based on city and profile
    customers_df = customers_df.withColumn(
        "current_state",
        when(col("current_city") == "Mumbai", "Maharashtra")
        .when(col("current_city") == "Delhi", "Delhi")
        .when(col("current_city") == "Bangalore", "Karnataka")
        .when(col("current_city") == "Chennai", "Tamil Nadu")
        .when(col("current_city") == "Kolkata", "West Bengal")
        .when(col("current_city") == "Hyderabad", "Telangana")
        .when(col("current_city") == "Pune", "Maharashtra")
        .when(col("current_city") == "Ahmedabad", "Gujarat")
        .when(col("current_city") == "Jaipur", "Rajasthan")
        .when(col("current_city") == "Kota", "Rajasthan")
        .otherwise("Various")
    )
    
    # Calculate income based on city distribution
    city_income_map = {city: info['avg_income'] for city, info in config.CITY_DISTRIBUTION.items()}
    
    customers_df = customers_df.withColumn(
        "annual_income",
        when(col("current_city") == "Mumbai", 
             abs(randn() * 200000 + 450000))
        .when(col("current_city") == "Delhi",
             abs(randn() * 180000 + 420000))
        .when(col("current_city") == "Bangalore",
             abs(randn() * 220000 + 480000))
        .when(col("current_city") == "Kota",
             abs(randn() * 70000 + 180000))
        .otherwise(abs(randn() * 100000 + 250000))
    )
    
    # Calculate CIBIL score based on customer profile
    customers_df = customers_df.withColumn(
        "cibil_score",
        when(col("customer_profile") == "excellent",
             (rand() * 100 + 750).cast("int"))
        .when(col("customer_profile") == "good",
             (rand() * 99 + 650).cast("int"))
        .when(col("customer_profile") == "fair",
             (rand() * 99 + 550).cast("int"))
        .otherwise((rand() * 249 + 300).cast("int"))
    )
    
    # Add data quality issues
    customers_df = customers_df.withColumn(
        "mobile_number",
        when(rand() < 0.05, None)  # 5% missing mobile numbers
        .otherwise(col("mobile_number"))
    )
    
    customers_df = customers_df.withColumn(
        "email",
        when(rand() < 0.03, concat(col("email"), lit("invalid")))  # 3% invalid emails
        .otherwise(col("email"))
    )
    
    return customers_df

# =====================================================
# LOANS TABLE GENERATION
# =====================================================

def generate_loans(spark, config, customers_df, institutions_df):
    """Generate realistic loan data with complex business logic"""
    
    # Create loan applications - some customers have multiple loans
    loan_spec = (
        dg.DataGenerator(spark, name="loans", rows=config.TOTAL_LOANS)
        .withIdOutput()
        .withColumn("loan_application_number", "string", template=r'LOAN\d{10}', random=True)
        
        # Link to customers (some customers have multiple loans)
        .withColumn("customer_id", "int", minValue=1, maxValue=config.TOTAL_CUSTOMERS)
        
        # Loan amounts - realistic distribution
        .withColumn("requested_amount", "decimal(15,2)",
                   minValue=50000, maxValue=2500000,
                   distribution="normal", mean=400000, stddev=300000)
        
        # Interest rates based on risk
        .withColumn("base_interest_rate", "decimal(6,3)",
                   minValue=7.5, maxValue=18.0,
                   distribution="normal", mean=12.0, stddev=2.5)
        
        # Application dates - realistic seasonal patterns
        .withColumn("application_date", "date",
                   begin=config.BUSINESS_START_DATE, end=config.CURRENT_DATE,
                   random=True)
        
        # Institution selection
        .withColumn("institution_id", "int", minValue=1, maxValue=len(config.INSTITUTION_TYPES))
        
        # Loan purposes
        .withColumn("loan_purpose", "string",
                   values=["Tuition Fee", "Hostel Fee", "Equipment", "Books & Stationery", "Examination Fee", "Living Expenses"],
                   weights=[0.60, 0.15, 0.10, 0.05, 0.05, 0.05])
        
        # Course duration
        .withColumn("course_duration_months", "int",
                   values=[12, 24, 36, 48, 60],
                   weights=[0.1, 0.2, 0.3, 0.3, 0.1])
    )
    
    loans_df = loan_spec.build()
    
    # Join with customer data for risk assessment
    loans_df = loans_df.join(
        customers_df.select("id", "customer_profile", "cibil_score", "annual_income", "current_city"),
        loans_df.customer_id == customers_df.id,
        "left"
    ).drop("id")
    
    # Calculate loan approval logic
    loans_df = loans_df.withColumn(
        "approval_probability",
        when(col("cibil_score") >= 750, 0.95)
        .when(col("cibil_score") >= 650, 0.80)
        .when(col("cibil_score") >= 550, 0.60)
        .otherwise(0.30)
    )
    
    loans_df = loans_df.withColumn(
        "loan_status",
        when(rand() < col("approval_probability"), "Approved")
        .otherwise("Rejected")
    )
    
    # Only process approved loans further
    approved_loans = loans_df.filter(col("loan_status") == "Approved")
    
    # Calculate sanctioned amount (usually less than requested)
    approved_loans = approved_loans.withColumn(
        "sanctioned_amount",
        least(col("requested_amount"), col("annual_income") * 2.5)
    )
    
    approved_loans = approved_loans.withColumn(
        "loan_amount", col("sanctioned_amount")
    )
    
    # Risk categorization
    approved_loans = approved_loans.withColumn(
        "risk_category",
        when(col("customer_profile") == "excellent", "Low")
        .when(col("customer_profile") == "good", "Medium")
        .when(col("customer_profile") == "fair", "High")
        .otherwise("Critical")
    )
    
    # Calculate EMI
    approved_loans = approved_loans.withColumn(
        "loan_term_months",
        when(col("course_duration_months") <= 24, col("course_duration_months") + 24)
        .otherwise(col("course_duration_months") + 36)
    )
    
    # EMI calculation using standard formula
    approved_loans = approved_loans.withColumn(
        "monthly_interest_rate", col("base_interest_rate") / 100 / 12
    )
    
    approved_loans = approved_loans.withColumn(
        "emi_amount",
        (col("loan_amount") * col("monthly_interest_rate") * 
         pow(1 + col("monthly_interest_rate"), col("loan_term_months"))) /
        (pow(1 + col("monthly_interest_rate"), col("loan_term_months")) - 1)
    )
    
    # Disbursement dates (after approval)
    approved_loans = approved_loans.withColumn(
        "disbursement_date",
        date_add(col("application_date"), expr("int(rand() * 45 + 15)"))
    )
    
    # Current loan status based on time and customer behavior
    current_date = datetime.now()
    approved_loans = approved_loans.withColumn(
        "months_since_disbursement",
        months_between(lit(current_date), col("disbursement_date"))
    )
    
    # Determine current loan status
    default_rates = {
        'Low': 0.03,
        'Medium': 0.08,
        'High': 0.18,
        'Critical': 0.35
    }
    
    approved_loans = approved_loans.withColumn(
        "default_probability",
        when(col("risk_category") == "Low", 0.03)
        .when(col("risk_category") == "Medium", 0.08)
        .when(col("risk_category") == "High", 0.18)
        .otherwise(0.35)
    )
    
    approved_loans = approved_loans.withColumn(
        "current_loan_status",
        when(col("months_since_disbursement") < 0, "Sanctioned")
        .when(rand() < col("default_probability"), "Defaulted")
        .when(col("months_since_disbursement") >= col("loan_term_months"), "Closed")
        .otherwise("Active")
    )
    
    return approved_loans

# =====================================================
# PAYMENTS TABLE GENERATION
# =====================================================

def generate_payments(spark, config, loans_df):
    """Generate realistic payment data with behavioral patterns"""
    
    # Filter only disbursed loans
    active_loans = loans_df.filter(
        (col("current_loan_status").isin(["Active", "Closed", "Defaulted"])) &
        (col("disbursement_date").isNotNull())
    )
    
    payments_data = []
    payment_id = 1
    
    # Convert to pandas for complex payment generation logic
    loans_pandas = active_loans.toPandas()
    
    for _, loan in loans_pandas.iterrows():
        loan_start_date = loan['disbursement_date']
        if pd.isna(loan_start_date):
            continue
            
        emi_amount = loan['emi_amount']
        loan_status = loan['current_loan_status']
        customer_profile = loan['customer_profile']
        
        # Determine payment behavior based on customer profile
        if customer_profile == 'excellent':
            on_time_prob = 0.95
            max_delay = 7
        elif customer_profile == 'good':
            on_time_prob = 0.85
            max_delay = 15
        elif customer_profile == 'fair':
            on_time_prob = 0.70
            max_delay = 30
        else:  # poor
            on_time_prob = 0.50
            max_delay = 60
        
        # Generate payments based on loan status
        current_date = datetime.now().date()
        payment_date = loan_start_date + timedelta(days=30)  # First EMI after 30 days
        outstanding_principal = loan['loan_amount']
        
        months_to_generate = min(
            int((current_date - loan_start_date).days / 30),
            loan['loan_term_months']
        )
        
        # If defaulted, stop payments at random point
        if loan_status == 'Defaulted':
            months_to_generate = random.randint(1, max(1, months_to_generate // 2))
        
        for month in range(months_to_generate):
            if outstanding_principal <= 0:
                break
                
            # Determine if payment is on time
            is_on_time = random.random() < on_time_prob
            
            if is_on_time:
                days_late = 0
                payment_status = 'Successful'
            else:
                days_late = random.randint(1, max_delay)
                # Higher chance of failure for very late payments
                if days_late > 30 and random.random() < 0.3:
                    payment_status = 'Failed'
                    payment_amount = 0
                else:
                    payment_status = 'Successful'
                    # Partial payments for late payments sometimes
                    if days_late > 15 and random.random() < 0.2:
                        payment_amount = emi_amount * random.uniform(0.5, 0.9)
                    else:
                        payment_amount = emi_amount
            
            # Calculate interest and principal components
            monthly_interest_rate = loan['base_interest_rate'] / 100 / 12
            interest_component = outstanding_principal * monthly_interest_rate
            
            if payment_status == 'Successful':
                if payment_amount == 0:
                    payment_amount = emi_amount
                    
                principal_component = min(
                    payment_amount - interest_component,
                    outstanding_principal
                )
                outstanding_principal -= principal_component
            else:
                payment_amount = 0
                principal_component = 0
                interest_component = 0
            
            # Add seasonal patterns (higher delays in March-April exam season)
            if payment_date.month in [3, 4] and random.random() < 0.3:
                days_late += random.randint(3, 10)
            
            payments_data.append({
                'payment_id': payment_id,
                'loan_id': loan['loan_id'],
                'payment_date': payment_date + timedelta(days=days_late),
                'due_date': payment_date,
                'payment_amount': round(payment_amount, 2),
                'principal_amount': round(principal_component, 2),
                'interest_amount': round(interest_component, 2),
                'penalty_amount': max(0, days_late * 100) if days_late > 0 else 0,
                'days_early_late': days_late,
                'payment_sequence_number': month + 1,
                'total_outstanding_principal': round(outstanding_principal, 2),
                'payment_method': random.choices(
                    ['Auto Debit', 'UPI', 'Net Banking', 'Cheque', 'Cash'],
                    weights=[0.4, 0.3, 0.2, 0.08, 0.02]
                )[0],
                'payment_status': payment_status,
                'transaction_reference': f"TXN{payment_id:010d}"
            })
            
            payment_id += 1
            payment_date += timedelta(days=30)  # Next month
            
            # Stop if loan is fully paid
            if outstanding_principal <= 100:  # Small remaining balance
                break
    
    # Convert back to Spark DataFrame
    payments_df = spark.createDataFrame(payments_data)
    return payments_df

# =====================================================
# DEFAULTS TABLE GENERATION
# =====================================================

def generate_defaults(spark, config, loans_df, payments_df):
    """Generate realistic default and collection data"""
    
    # Find defaulted loans
    defaulted_loans = loans_df.filter(col("current_loan_status") == "Defaulted")
    
    # Get last payment information for each defaulted loan
    last_payments = payments_df.groupBy("loan_id").agg(
        max("payment_date").alias("last_payment_date"),
        max("payment_sequence_number").alias("last_payment_sequence"),
        sum("payment_amount").alias("total_paid")
    )
    
    # Join with defaulted loans
    defaults_base = defaulted_loans.join(
        last_payments, "loan_id", "left"
    )
    
    # Calculate default information
    defaults_df = defaults_base.withColumn(
        "default_date",
        coalesce(
            date_add(col("last_payment_date"), 60),  # Default 60 days after last payment
            date_add(col("disbursement_date"), 90)   # Or 90 days after disbursement
        )
    )
    
    defaults_df = defaults_df.withColumn(
        "dpd_days",
        datediff(current_date(), col("default_date"))
    )
    
    # Categorize into buckets
    defaults_df = defaults_df.withColumn(
        "default_bucket",
        when(col("dpd_days") <= 30, "0-30 DPD")
        .when(col("dpd_days") <= 60, "31-60 DPD")
        .when(col("dpd_days") <= 90, "61-90 DPD")
        .when(col("dpd_days") <= 180, "91-180 DPD")
        .otherwise("180+ DPD")
    )
    
    # Calculate default amount
    defaults_df = defaults_df.withColumn(
        "default_amount",
        col("loan_amount") - coalesce(col("total_paid"), 0)
    )
    
    # Add default reasons based on customer profile and timing
    defaults_df = defaults_df.withColumn(
        "primary_default_reason",
        when(year(col("default_date")).isin([2020, 2021]), "Job Loss")  # COVID impact
        .when(col("customer_profile") == "poor", "Income Reduction")
        .when(month(col("default_date")).isin([3, 4]), "Course Dropout")  # Exam season
        .otherwise(
            expr("array('Family Issues', 'Medical Emergency', 'Business Failure', 'Other')[int(rand() * 4)]")
        )
    )
    
    # Collection stage based on time in default
    defaults_df = defaults_df.withColumn(
        "collection_stage",
        when(col("dpd_days") <= 30, "Early Collection")
        .when(col("dpd_days") <= 90, "Primary Collection")
        .when(col("dpd_days") <= 180, "Secondary Collection")
        .otherwise("Legal Action")
    )
    
    # Recovery information
    defaults_df = defaults_df.withColumn(
        "recovery_percentage",
        when(col("default_bucket") == "0-30 DPD", rand() * 30 + 70)  # 70-100% recovery
        .when(col("default_bucket") == "31-60 DPD", rand() * 40 + 40)  # 40-80% recovery
        .when(col("default_bucket") == "61-90 DPD", rand() * 30 + 20)  # 20-50% recovery
        .when(col("default_bucket") == "91-180 DPD", rand() * 20 + 10)  # 10-30% recovery
        .otherwise(rand() * 15)  # 0-15% recovery
    )
    
    defaults_df = defaults_df.withColumn(
        "total_recovered_amount",
        col("default_amount") * col("recovery_percentage") / 100
    )
    
    return defaults_df

# =====================================================
# MAIN DATA GENERATION FUNCTION
# =====================================================

def generate_edufin_dataset(spark):
    """Main function to generate complete EduFin dataset"""
    
    config = EduFinDataConfig()
    
    print("Generating EduFin Industrial Dataset...")
    print(f"Target size: {config.TOTAL_CUSTOMERS:,} customers, {config.TOTAL_LOANS:,} loans, {config.TOTAL_PAYMENTS:,} payments")
    
    # Generate institutions
    print("1. Generating institutions data...")
    institutions_df = generate_institutions(spark, config)
    institutions_df.cache()
    print(f"   Generated {institutions_df.count():,} institutions")
    
    # Generate customers
    print("2. Generating customers data...")
    customers_df = generate_customers(spark, config)
    customers_df.cache()
    print(f"   Generated {customers_df.count():,} customers")
    
    # Generate loans
    print("3. Generating loans data...")
    loans_df = generate_loans(spark, config, customers_df, institutions_df)
    loans_df.cache()
    print(f"   Generated {loans_df.count():,} loans")
    
    # Generate payments
    print("4. Generating payments data...")
    payments_df = generate_payments(spark, config, loans_df)
    payments_df.cache()
    print(f"   Generated {payments_df.count():,} payments")
    
    # Generate defaults
    print("5. Generating defaults data...")
    defaults_df = generate_defaults(spark, config, loans_df, payments_df)
    defaults_df.cache()
    print(f"   Generated {defaults_df.count():,} defaults")
    
    # Validate data quality
    print("\n6. Data Quality Validation:")
    
    # Check default rate
    total_loans = loans_df.count()
    defaulted_loans = loans_df.filter(col("current_loan_status") == "Defaulted").count()
    default_rate = (defaulted_loans / total_loans) * 100
    print(f"   Default rate: {default_rate:.2f}% (target: ~12%)")
    
    # Check geographic distribution
    city_dist = customers_df.groupBy("current_city").count().orderBy(desc("count"))
    print(f"   Top 5 cities by customer count:")
    for row in city_dist.take(5):
        print(f"     {row['current_city']}: {row['count']:,}")
    
    # Check payment behavior
    payment_stats = payments_df.groupBy("payment_status").count().orderBy(desc("count"))
    print(f"   Payment status distribution:")
    for row in payment_stats.collect():
        print(f"     {row['payment_status']}: {row['count']:,}")
    
    print("\n7. Saving datasets...")
    
    # Save as Parquet for efficient loading
    output_path = "/tmp/edufin_data"
    
    institutions_df.write.mode("overwrite").parquet(f"{output_path}/institutions")
    customers_df.write.mode("overwrite").parquet(f"{output_path}/customers")
    loans_df.write.mode("overwrite").parquet(f"{output_path}/loans")
    payments_df.write.mode("overwrite").parquet(f"{output_path}/payments")
    defaults_df.write.mode("overwrite").parquet(f"{output_path}/defaults_collections")
    
    print(f"   Data saved to {output_path}")
    print("   Ready for import to SSMS or other SQL databases")
    
    return {
        'institutions': institutions_df,
        'customers': customers_df,
        'loans': loans_df,
        'payments': payments_df,
        'defaults': defaults_df
    }

# =====================================================
# EXPORT TO CSV FOR SSMS IMPORT
# =====================================================

def export_for_ssms_import(datasets, output_path="/tmp/edufin_csv"):
    """Export datasets as CSV for SSMS import"""
    
    print(f"\nExporting CSV files for SSMS import to {output_path}...")
    
    for table_name, df in datasets.items():
        csv_path = f"{output_path}/{table_name}.csv"
        
        # Convert to pandas for CSV export with proper formatting
        pandas_df = df.toPandas()
        
        # Handle date formatting for SSMS
        date_columns = [col for col in pandas_df.columns if 'date' in col.lower()]
        for col in date_columns:
            if col in pandas_df.columns:
                pandas_df[col] = pd.to_datetime(pandas_df[col]).dt.strftime('%Y-%m-%d')
        
        # Export to CSV
        pandas_df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"   Exported {table_name}: {len(pandas_df):,} rows to {csv_path}")
    
    print("\nCSV files ready for SSMS import!")
    print("Use SQL Server Import/Export Wizard or BULK INSERT to load data.")

# =====================================================
# USAGE EXAMPLE
# =====================================================

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EduFin_Industrial_DataGen") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Generate complete dataset
    datasets = generate_edufin_dataset(spark)
    
    # Export for SSMS import
    export_for_ssms_import(datasets)
    
    # Optional: Show sample data
    print("\nSample data preview:")
    for table_name, df in datasets.items():
        print(f"\n{table_name.upper()} (showing 5 rows):")
        df.show(5, truncate=False)
    
    spark.stop()

# =====================================================
# CONFIGURATION FOR PRODUCTION USE
# =====================================================

"""
Production Configuration Notes:

1. Databricks Setup:
   - Use Databricks Community Edition (free)
   - Runtime: 13.3 LTS ML (includes dbldatagen)
   - Cluster: Single node with 8GB memory minimum

2. Data Generation:
   - Run in chunks if memory constrained
   - Use .cache() strategically for large datasets
   - Monitor Spark UI for performance optimization

3. Export Options:
   - CSV for SSMS import (most compatible)
   - Parquet for fastest loading
   - Delta format for advanced capabilities

4. Quality Assurance:
   - Always validate business logic after generation
   - Check data distributions match expectations
   - Verify referential integrity between tables

5. Customization:
   - Adjust TOTAL_* constants for different dataset sizes
   - Modify business rules in EduFinDataConfig
   - Add additional data quality issues as needed

6. Performance Tips:
   - Use broadcast joins for small lookup tables
   - Partition large tables by date for better performance
   - Consider using Spark SQL for complex business logic
"""