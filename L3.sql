--L3_contract:
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L3.L3_contract` AS
SELECT contract_id
,branch_id
,contract_valid_from
,contract_valid_to
,CASE 
  WHEN DATE_DIFF(contract_valid_from, contract_valid_to, month)< 6 THEN 'less than half year'
  WHEN DATE_DIFF(contract_valid_from, contract_valid_to, month)<= 12 THEN 'up to 1 year'
  WHEN DATE_DIFF(contract_valid_from, contract_valid_to, month) > 12 THEN 'up to 2 years'
  WHEN DATE_DIFF(contract_valid_from, contract_valid_to, month)>= 24 THEN 'more than 2 years'
  END AS contract_duration  -- dopocitani delky trvani kontraktu
,registration_end_reason
,EXTRACT(year from contract_valid_from) AS  start_year_of_contract
,contract_status
,flag_prolongation
FROM `genial-shore-455518-j8.L2.L2_contract`
WHERE contract_valid_to > contract_valid_from 
  AND (contract_valid_from IS NOT NULL OR contract_valid_to IS NOT NULL)
--vyrazeni neplatnych kontraktu
;

--L3_invoice
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L3.L3_invoice` AS
SELECT invoice_id
,contract_id
,amount_w_vat
,return_w_vat
,amount_w_vat - return_w_vat AS total_usd_paid --dopocitani celkove zaplacene castky
,paid_date
FROM `genial-shore-455518-j8.L2.L2_invoice`


;
--L3_product_purchase 
-- Product_purchase pouzit místo tabulky product, jelikoz jsme jiz ve vrstve L1 napojili k teto tabulce tabulku product k doplneni informaci, je tedy ted dostacujici.
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L3.L3_product_purchase` AS
SELECT product_purchase_id --PK
,contract_id
,product_id --FK
,product_name
,product_type
,product_valid_from 
,product_valid_to
,unit 
,flag_unlimited_product
FROM `genial-shore-455518-j8.L2.L2_product_purchase`
WHERE product_name IS NOT NULL -- filtrace neplatnych produktu

;
--L3_branch
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L3.L3_branch` AS
SELECT branch_id
,branch_name
FROM `genial-shore-455518-j8.L2.L2_branch`
