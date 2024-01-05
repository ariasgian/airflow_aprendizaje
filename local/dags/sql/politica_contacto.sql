CREATE OR REPLACE PROCEDURE `teco-dev-cdp-056e.test_gian.politica_contacto`()
/*
  GENERACION DE POLITICA DE CONTACTO
  ACDP-983 Challenge Política de Contacto CAMBIO 0= GIAN CARLOS ARIAS
  GENERA AUDIENCIA PARA SUBIR A LOOKER
*/
BEGIN
  DECLARE fecha_entidad DATE; #fecha Parque
  declare ult_fecha_contact DATE;
  declare ult_fecha_mora DATE;
  SET @@dataset_project_id = 'teco-dev-cdp-056e';
  SET @@dataset_id = 'test_gian';

  SET ult_fecha_mora = (
    SELECT 
      MAX(PARSE_DATE('%Y%m%d', CAST(partition_id AS STRING))) AS FECHA
    FROM 
      `teco-prod-cdp-8d52.audiencias.INFORMATION_SCHEMA.PARTITIONS`
    WHERE 
      table_name = 'audiencia_mora_fan_historico'
      AND total_rows > 0
      AND partition_id <> '__NULL__'
  );


  delete STG_FAN_NPLAY_UNION
  where fecha_foto >= current_date-2;
  #IF (ult_fecha_contact < CURRENT_DATE) or (ult_fecha_contact is null) THEN
    # 01 Unifica las tablas de las contact para uso posterior
  INSERT INTO `teco-prod-cdp-8d52.stg_fan_movil.STG_FAN_NPLAY_UNION` 
  SELECT 
    linea AS CUSTOMER_ID,
    REFERENTE_PAGO,
    NUMERO_DOCUMENTO,
    tipo_doc,
    current_date as fecha_foto,
    'fan' as tipo
  FROM 
    `teco-prod-cdp-8d52.stg_fan_movil.Contact_List_Fan` FAN
  union all
  select
    ACCOUNT_INTEGRATION_ID AS CUSTOMER_ID,
    ACCOUNT_INTEGRATION_ID AS REFERENTE_PAGO,
    NUMERODOCUMENTO AS NUMERO_DOCUMENTO,
    TIPODOCUMENTO AS tipo_doc,
    current_date as fecha_foto,
    'nplay' as tipo
  FROM
    `teco-prod-cdp-8d52.audiencias.PARQUE_NPLAY_CONT_LIST` NPLAY
  where
    NPLAY.FECHA_PROCESO= current_date-1
  ;
  create or replace table audiencias.politica_contactacion as
    WITH contact AS(
      SELECT
        CUSTOMER_ID,
        REFERENTE_PAGO,
        NUMERO_DOCUMENTO
      FROM `teco-prod-cdp-8d52.stg_fan_movil.STG_FAN_NPLAY_UNION` 
      where
        fecha_foto = current_date
    ),
    mora as(
      select
        CUSTOMER_ID,
        FECHA_MORA_HITO_FAN,
        INSTANCIA_CBS_FAN
      FROM
        `teco-prod-cdp-8d52.audiencias.audiencia_mora_fan_historico`
      WHERE
        FECHA_FOTO= ult_fecha_mora
    ),
    ppay as (
      SELECT
        DOC_NUMBER,
        EMAIL
      FROM
        `teco-prod-cdp-8d52.PersonalPay.parque_fintech`
    ),
    factu as(
      select
        NUMERO_LINEA as CUSTOMER_ID,
        CICLO_FACTURACION,
        FECHA_EMISION_FACTURA,
        FECHA_VTO
      FROM
        `teco-prod-datalake-8f6a.vw_camp_facturas_emailing.facturas_fan`
      where
        FECHA_EMISION_FACTURA >= CURRENT_DATE-10
    )
    SELECT  
      contact.CUSTOMER_ID,
      contact.REFERENTE_PAGO,
      mora.INSTANCIA_CBS_FAN,
      case
        when mora.FECHA_MORA_HITO_FAN is not null then 1
        else 0
      end FL_MORA,
      case
        when DATE_DIFF(current_date, factu.FECHA_EMISION_FACTURA,DAY) between 4 and 9 then 1
        else 0
      end FL_FACTURACION,
      case
        when ppay.DOC_NUMBER is not null then 1
        else 0
      end FL_FINTECH
    FROM contact
    left join mora USING(CUSTOMER_ID)
    LEFT JOIN factu
    ON 
      contact.CUSTOMER_ID  = factu.CUSTOMER_ID
    left join ppay
    on 
      contact.NUMERO_DOCUMENTO= ppay.DOC_NUMBER
  ;
END