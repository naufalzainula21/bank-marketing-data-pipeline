-- FROM SOURCE DATABASE
CREATE TABLE marital_status (
    marital_id SERIAL PRIMARY KEY,
    value VARCHAR(50)  UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE education_status (
    education_id SERIAL PRIMARY KEY,
    value VARCHAR(50)  UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marketing_campaign_deposit   (
    loan_data_id SERIAL PRIMARY KEY,
    age INT ,
    job VARCHAR(100) ,
    marital_id INT ,
    education_id INT ,
    "default" BOOLEAN ,
    balance INT , 
    housing BOOLEAN ,
    loan BOOLEAN ,
    contact VARCHAR(50),
    day INT ,
    month VARCHAR(10) ,
    duration INT ,
    duration_in_year INT, 
    campaign INT ,
    days_since_last_campaign INT , 
    previous_campaign_contacts INT ,
    previous_campaign_outcome VARCHAR(50), 
    subscribed_deposit BOOLEAN ,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (marital_id) REFERENCES marital_status(marital_id) ON DELETE CASCADE,
    FOREIGN KEY (education_id) REFERENCES education_status(education_id) ON DELETE CASCADE
);

-- FROM SOURCE CSV
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    birth_date DATE ,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other')),
    location VARCHAR(255) ,
    account_balance FLOAT , 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR ,
    transaction_date DATE ,
    transaction_time VARCHAR ,
    transaction_amount FLOAT ,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

