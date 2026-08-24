-- Column masking + row-level security.
-- Same table, same query, different results depending on who is asking - which
-- means analysts can be given broad access without exposing raw PII.

USE CATALOG lakehouse_dev;

CREATE OR REPLACE FUNCTION governance.mask_email(email STRING)
  RETURN CASE
    WHEN is_account_group_member('data_pii_readers') THEN email
    WHEN email IS NULL THEN NULL
    ELSE concat(left(email, 1), '****@', split_part(email, '@', 2))
  END;

CREATE OR REPLACE FUNCTION governance.mask_phone(phone STRING)
  RETURN CASE
    WHEN is_account_group_member('data_pii_readers') THEN phone
    WHEN phone IS NULL THEN NULL
    ELSE concat('******', right(phone, 4))
  END;

CREATE OR REPLACE FUNCTION governance.mask_name(name STRING)
  RETURN CASE
    WHEN is_account_group_member('data_pii_readers') THEN name
    ELSE sha2(name, 256)
  END;

-- Row filter: analysts see only their own region unless explicitly global.
CREATE OR REPLACE FUNCTION governance.row_filter_country(country STRING)
  RETURN is_account_group_member('data_global_readers')
      OR is_account_group_member('data_platform_admins')
      OR country = 'IN';

-- Attach the policies.
ALTER TABLE silver.silver_customers ALTER COLUMN email     SET MASK governance.mask_email;
ALTER TABLE silver.silver_customers ALTER COLUMN phone     SET MASK governance.mask_phone;
ALTER TABLE silver.silver_customers ALTER COLUMN full_name SET MASK governance.mask_name;

ALTER TABLE gold.dim_customer ALTER COLUMN email     SET MASK governance.mask_email;
ALTER TABLE gold.dim_customer ALTER COLUMN full_name SET MASK governance.mask_name;

ALTER TABLE gold.dim_customer SET ROW FILTER governance.row_filter_country ON (country);

-- Verify as a non-privileged user:
--   SELECT customer_id, full_name, email FROM gold.dim_customer LIMIT 5;
