-- L2_contracts
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L2.L2_contract` AS
SELECT 
contract_id --PK
, branch_id --FK
, contract_valid_from
, contract_valid_to
, registered_date
, signed_date
, activation_process_date
, prolongation_date
, registration_end_reason
, flag_prolongation
, flag_send_email
, contract_status
FROM `genial-shore-455518-j8.L1_crm.L1_contracts`
WHERE registered_date IS NOT NULL
;

--L2_invoice
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L2.L2_invoice` AS
SELECT
  invoice_id -- PK
, invoice_old_id
, contract_id -- FK
, invoice_type
, invoice_status_id
, amount_w_vat
, IF(amount_w_vat <= 0, 0, amount_w_vat/1.2) AS amount_wo_vat
, return_w_vat
, flag_invoice_issued
, date_issue
, due_date
, paid_date
, start_date
, end_date
, insert_date
, update_date
, amount_payed
, flag_paid_currier
, invoice_number
, ROW_NUMBER () OVER (PARTITION BY contract_id ORDER BY date_issue) AS invoice_order
FROM `genial-shore-455518-j8.L1_accounting_system.L1_invoice`
WHERE invoice_type_id = 1 -- invoice type = 'invoice'
  AND flag_invoice_issued IS TRUE
;


--L2_product
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L2.L2_product` AS
SELECT product_id
,product_name
,product_type
,product_category
FROM `genial-shore-455518-j8.L1_google_sheets.L1_product`
WHERE product_category IN ('product', 'rent');

--L2_branch
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L2.L2_branch` AS
SELECT
branch_id
,branch_name
FROM `genial-shore-455518-j8.L1.L1_branch`
WHERE branch_name <> 'unknown';

--L2_product_purchase
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L2.L2_product_purchase` AS
SELECT 
product_purchase_id
,product_id
,contract_id
,product_category
,product_status
,price_wo_vat
,price_wo_vat * 1.2 AS price_w_vat
,product_valid_from
,product_valid_to
,IF(product_valid_from = '2035-12-31', TRUE, FALSE) AS flag_unlimited_product
,unit
,product_name
,product_type
,create_date
,update_date
FROM `genial-shore-455518-j8.L1.L1_product_purchase`
WHERE product_category IN ('product','rent') 
  AND product_status IS NOT NULL 
  AND product_status NOT IN ('canceled','disconnected');
