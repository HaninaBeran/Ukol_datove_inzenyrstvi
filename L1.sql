-- L1 status
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_google_sheets.L1_status` AS
SELECT
CAST(id_status AS INT) AS product_status_id
, LOWER(status_name) AS product_status_name
, DATE (TIMESTAMP (date_update), "Europe/Prague") AS product_status_date -- protože to házelo chybu, doplnily jsme tam TIMESTAMP
FROM `genial-shore-455518-j8.L0_google_sheets.status`
WHERE id_status IS NOT NULL 
  AND status_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_status_id) = 1 --unique ID
;



-- L1_invoice
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_accounting_system.L1_invoice` AS
SELECT
  id_invoice AS invoice_id -- PK
, id_invoice_old AS invoice_old_id
, invoice_id_contract AS contract_id -- FK
, status AS invoice_status_id
, id_branch AS branch_id -- FK
-- Invoice status. Invoice status < 100  have been issued. >= 100 - not issued
, IF (status < 100, TRUE, FALSE) AS flag_invoice_issued
, DATE (date, "Europe/Prague" ) AS date_issue
, DATE (scadent, "Europe/Prague" ) AS due_date
, DATE (date_paid, "Europe/Prague" ) AS paid_date
, DATE (start_date, "Europe/Prague" ) AS start_date
, DATE (end_date, "Europe/Prague" ) AS end_date
, DATE (date_insert, "Europe/Prague" ) AS insert_date
, DATE (date_update, "Europe/Prague" ) AS update_date
, value AS amount_w_vat
, payed AS amount_payed
, flag_paid_currier
, invoice_type as invoice_type_id -- Invoice_type: 1 - invoice, 3 - credit_note, 2- return, 4 - other  (v dokumentaci)
, CASE
  WHEN invoice_type = 1 THEN "invoice"
  WHEN invoice_type = 2 THEN "return"
  WHEN invoice_type = 3 THEN "credit_note"
  WHEN invoice_type = 4 THEN "other"
END AS invoice_type
, number AS invoice_number
, value_storno AS return_w_vat
FROM `genial-shore-455518-j8.L0_accounting_system.invoice`
WHERE id_invoice IS NOT NULL 
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_invoice) = 1 
;



-- L1_invoices_load
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_accounting_system.L1_invoices_load` AS
SELECT
  id_load AS invoice_load_id
  ,id_contract AS contract_id -- FK
  ,CAST(id_package AS INT) AS package_id --FK
  ,id_invoice AS invoice_id --FK
  ,CAST(id_package_template AS INT) AS product_id -- FK
  ,notlei AS price_wo_vat_usd --(notlei x 100000) AS price_wo_vat_usd
  ,tva AS vat_rate
  ,value AS price_w_vat_usd
  ,payed AS paid_w_vat_usd
  ,currency
  ,case 
    when um IN ('mesia','m?síce','m?si?1ce','měsice','mesiace','měsíce','mesice') then  'month'
    when um = "kus" then "item"
    when um = "den" then 'day'
    when um = '0' then null 
    else um end AS unit
  ,quantity
  ,DATE (TIMESTAMP (start_date), "Europe/Prague" ) AS start_date
  ,DATE (TIMESTAMP (end_date), "Europe/Prague" ) AS end_date
  ,DATE (TIMESTAMP (date_insert), "Europe/Prague" ) AS date_insert
  ,DATE (TIMESTAMP (date_update), "Europe/Prague" ) AS date_update
  ,DATE (TIMESTAMP (load_date), "Europe/Prague" ) AS load_date
FROM `genial-shore-455518-j8.L0_accounting_system.invoices_load`
WHERE id_load IS NOT NULL 
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_load) = 1 
;


-- L1_contracts
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_crm.L1_contracts` AS
SELECT 
id_contract AS contract_id --PK
, id_branch AS branch_id --FK
, DATE (TIMESTAMP (date_contract_valid_from), "Europe/Prague") AS contract_valid_from
, DATE (TIMESTAMP (date_contract_valid_to), "Europe/Prague") AS contract_valid_to
, DATE (TIMESTAMP (date_registered), "Europe/Prague") AS registered_date
, DATE (TIMESTAMP (date_signed), "Europe/Prague")AS signed_date
, DATE (TIMESTAMP (activation_process_date), "Europe/Prague") AS activation_process_date
, DATE (TIMESTAMP (prolongation_date), "Europe/Prague") AS prolongation_date
, registration_end_reason
, flag_prolongation
, flag_send_inv_email as flag_send_email
, contract_status as contract_status
, DATE (TIMESTAMP (load_date), "Europe/Prague") AS load_date
FROM `genial-shore-455518-j8.L0_crm.contracts`
WHERE id_contract IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_contract) = 1 
;

-- L1_product
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_google_sheets.L1_product` AS
SELECT 
id_product AS product_id
, name AS product_name
, type AS product_type
, category AS product_category
, is_vat_applicable AS is_vat_applicable
, DATE (TIMESTAMP (date_update), "Europe/Prague") AS product_update_date
FROM `genial-shore-455518-j8.L0_google_sheets.all_products`
WHERE id_product IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_product) = 1
;

-- L1_branch -- nesmí být unknown (ale možné až na L2)
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_google_sheets.L1_branch` AS
SELECT
 CAST(id_branch AS INT) AS branch_id
,branch_name
, DATE (TIMESTAMP (date_update),"Europe/Prague") AS product_status_update_date
FROM `genial-shore-455518-j8.L0_google_sheets.branch`
WHERE id_branch IS NOT NULL 
  AND id_branch <> 'NULL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_branch) = 1 
;

-- L1_product_purchase 
CREATE OR REPLACE VIEW `genial-shore-455518-j8.L1_crm.L1_product_purchase` AS -- measure_unit do aj
SELECT 
id_package AS product_purchase_id
, id_contract AS contract_id
, id_package_template AS product_id
, DATE (TIMESTAMP (date_insert), "Europe/Prague") AS create_date
, DATE (TIMESTAMP (start_date), "Europe/Prague") AS product_valid_from
, DATE (TIMESTAMP (end_date), "Europe/Prague") AS product_valid_to
, fee AS price_wo_vat
, DATE (TIMESTAMP (pp.date_update), "Europe/Prague") AS update_date
, package_status AS product_status_id
, st.product_status_name AS product_status
, pr.product_name
, pr.product_type
, pr.product_category
, CASE 
    WHEN measure_unit = 'měsíce' THEN  'month'
    when measure_unit = "kus" then "item"
    when measure_unit = "den" then 'day'
    ELSE measure_unit
    END AS unit
, id_branch AS branch_id
, DATE (TIMESTAMP (load_date), "Europe/Prague") AS load_date
FROM `genial-shore-455518-j8.L0_crm.product_purchases` pp 
-- pro lepsi prehlednost pri prochazeni napojeny tabulky s informacemi o produktech
 LEFT JOIN `genial-shore-455518-j8.L1_google_sheets.L1_status` st ON st.product_status_id = pp.package_status
 LEFT JOIN `genial-shore-455518-j8.L1_google_sheets.L1_product` pr ON pr.product_id = pp.id_package_template
WHERE id_package IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_package) = 1 
;