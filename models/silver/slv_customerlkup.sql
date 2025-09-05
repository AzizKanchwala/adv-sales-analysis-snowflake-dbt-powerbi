WITH base AS (
    SELECT *
    FROM {{ ref('brn_customerlkup') }}
)

SELECT
    *,
    
    -- 1. Parent: Yes if TotalChildren > 0, else No
    CASE 
        WHEN TotalChildren > 0 THEN 'Yes'
        ELSE 'No'
    END AS Parent,
    
    -- 2. CustomerPriority: Priority if parent and AnnualIncome > 100000
    CASE 
        WHEN TotalChildren > 0 AND AnnualIncome > 100000 THEN 'Priority'
        ELSE 'Standard'
    END AS CustomerPriority,
    
    -- 3. IncomeLevel based on AnnualIncome
    CASE 
        WHEN AnnualIncome >= 150000 THEN 'Very High'
        WHEN AnnualIncome >= 100000 THEN 'High'
        WHEN AnnualIncome >= 50000 THEN 'Average'
        ELSE 'Low'
    END AS IncomeLevel,
    
    -- 4. EducationCategory mapping
    CASE 
        WHEN EducationLevel IN ('High School', 'Partial High School') THEN 'High School'
        WHEN EducationLevel IN ('Bachelors', 'Partial College') THEN 'Undergrad'
        WHEN EducationLevel = 'Graduate Degree' THEN 'Graduate'
        ELSE EducationLevel  -- keep original if unmatched
    END AS EducationCategory,
    
    -- 5. BirthYear extracted from BirthDate
    YEAR(BirthDate) AS BirthYear

FROM base