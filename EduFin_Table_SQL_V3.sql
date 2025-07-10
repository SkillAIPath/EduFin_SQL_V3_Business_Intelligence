CREATE TABLE geographic_demographics (
    state VARCHAR(100),
    city VARCHAR(100),
    tier_classification VARCHAR(10),
    population_18_35 INT,
    higher_education_enrollment INT,
    average_household_income INT,
    unemployment_rate FLOAT
);

INSERT INTO geographic_demographics VALUES
('Maharashtra', 'Mumbai', 'Tier1', 4500000, 120000, 850000, 4.5),
('Delhi', 'New Delhi', 'Tier1', 3000000, 95000, 820000, 4.2),
('Tamil Nadu', 'Chennai', 'Tier1', 2800000, 87000, 780000, 5.1),
('Karnataka', 'Bangalore', 'Tier1', 3200000, 90000, 800000, 5.3),
('West Bengal', 'Kolkata', 'Tier1', 3100000, 88000, 790000, 5.6),
('Gujarat', 'Ahmedabad', 'Tier1', 2700000, 85000, 770000, 4.8),
('Telangana', 'Hyderabad', 'Tier1', 2600000, 81000, 760000, 4.9),
('Uttar Pradesh', 'Lucknow', 'Tier2', 2500000, 75000, 620000, 7.2),
('Rajasthan', 'Jaipur', 'Tier2', 2200000, 68000, 590000, 6.8),
('Madhya Pradesh', 'Bhopal', 'Tier2', 2000000, 61000, 550000, 8.1),
('Bihar', 'Patna', 'Tier2', 1800000, 58000, 530000, 8.5),
('Jharkhand', 'Ranchi', 'Tier3', 1300000, 51000, 470000, 9.0),
('Odisha', 'Bhubaneswar', 'Tier2', 1500000, 54000, 490000, 7.5),
('Punjab', 'Ludhiana', 'Tier2', 1600000, 56000, 500000, 6.9),
('Haryana', 'Gurgaon', 'Tier2', 1700000, 57000, 520000, 5.6),
('Kerala', 'Kochi', 'Tier2', 1450000, 53000, 480000, 5.2),
('Assam', 'Guwahati', 'Tier3', 1200000, 49000, 450000, 8.8),
('Chhattisgarh', 'Raipur', 'Tier3', 1100000, 46000, 430000, 9.3),
('Uttarakhand', 'Dehradun', 'Tier3', 1000000, 44000, 410000, 7.9),
('Himachal Pradesh', 'Shimla', 'Tier3', 950000, 42000, 400000, 7.4);

select * from geographic_demographics;

CREATE TABLE economic_indicators (
    state VARCHAR(100),
    year INT,
    gdp_growth_rate FLOAT,
    inflation_rate FLOAT,
    employment_rate FLOAT,
    education_expenditure_percentage FLOAT
);

INSERT INTO economic_indicators VALUES
('Maharashtra', 2025, 7.8, 5.2, 94.5, 4.2),
('Delhi', 2025, 8.1, 5.0, 95.2, 4.5),
('Tamil Nadu', 2025, 7.5, 5.4, 93.8, 4.1),
('Karnataka', 2025, 8.3, 4.9, 95.0, 4.3),
('West Bengal', 2025, 7.0, 5.3, 92.4, 4.0),
('Gujarat', 2025, 7.6, 5.1, 93.0, 3.9),
('Telangana', 2025, 8.0, 5.0, 94.1, 4.2),
('Uttar Pradesh', 2025, 6.2, 5.6, 89.5, 3.9),
('Rajasthan', 2025, 5.9, 5.8, 90.0, 3.8),
('Madhya Pradesh', 2025, 5.5, 6.0, 88.0, 3.6),
('Bihar', 2025, 5.1, 6.1, 86.3, 3.5),
('Jharkhand', 2025, 4.8, 6.3, 85.5, 3.3),
('Odisha', 2025, 5.6, 5.9, 87.5, 3.7),
('Punjab', 2025, 6.8, 5.5, 91.0, 4.0),
('Haryana', 2025, 7.1, 5.2, 92.0, 4.1),
('Kerala', 2025, 6.7, 5.3, 91.5, 4.3),
('Assam', 2025, 4.9, 6.4, 84.5, 3.2),
('Chhattisgarh', 2025, 4.5, 6.5, 83.0, 3.1),
('Uttarakhand', 2025, 5.4, 5.7, 88.8, 3.4),
('Himachal Pradesh', 2025, 5.2, 5.8, 89.0, 3.5);

select * from economic_indicators;
