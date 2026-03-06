-- Converted from Greenplum to Spark SQL
-- View: vanderlande.vw_iata_tmp

CREATE OR REPLACE VIEW vanderlande.vw_iata_tmp AS 

SELECT      
  sha2(concat_ws('|',
    message_type,
    regexp_replace(date_format(COALESCE(inbound_flight_date, CAST('1900-01-01' AS DATE)), 'yyyyMMdd'), '19000101', '00000000'),
    
    CONCAT(
      regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[0-9]+$', ''),
      CASE
        WHEN length(regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', '')) < 4
          THEN lpad(regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', ''), 4, '0')
        ELSE regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', '')
      END
    ),
    
    regexp_replace(date_format(COALESCE(outbound_flight_date, CAST('1900-01-01' AS DATE)), 'yyyyMMdd'), '19000101', '00000000'),
    
    CONCAT(
      regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[0-9]+$', ''),
      CASE
        WHEN length(regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', '')) < 4
          THEN lpad(regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', ''), 4, '0')
        ELSE regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', '')
      END
    ),
    `[.N/] Baggage Tag Details`
  ), 256) AS hash,
  
  concat_ws('|',
    message_type,
    regexp_replace(date_format(COALESCE(inbound_flight_date, CAST('1900-01-01' AS DATE)), 'yyyyMMdd'), '19000101', '00000000'),
    
    CONCAT(
      regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[0-9]+$', ''),
      CASE
        WHEN length(regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', '')) < 4
          THEN lpad(regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', ''), 4, '0')
        ELSE regexp_replace(COALESCE(COALESCE(inbound_flight_number, '00000000'), ''), '[^0-9]', '')
      END
    ),
    
    regexp_replace(date_format(COALESCE(outbound_flight_date, CAST('1900-01-01' AS DATE)), 'yyyyMMdd'), '19000101', '00000000'),
    
    CONCAT(
      regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[0-9]+$', ''),
      CASE
        WHEN length(regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', '')) < 4
          THEN lpad(regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', ''), 4, '0')
        ELSE regexp_replace(COALESCE(COALESCE(outbound_flight_number, '00000000'), ''), '[^0-9]', '')
      END
    ),
    `[.N/] Baggage Tag Details`
  ) AS bhs_pk,
  
  *
  
FROM ( 
  SELECT 
    CASE 
      WHEN inbound_flight_date > outbound_flight_date_1 THEN
        CASE 
          WHEN inbound_flight_date > to_date(
            concat(
              date_format(eventts, 'yyyy'),
              date_format(outbound_flight_date_1, 'MMdd')
            ),
            'yyyyMMdd'
          )
          THEN to_date(
            concat(
              date_format(date_add(eventts, 1), 'yyyy'),
              date_format(outbound_flight_date_1, 'MMdd')
            ),
            'yyyyMMdd'
          )
          ELSE to_date(
            concat(
              date_format(eventts, 'yyyy'),
              date_format(outbound_flight_date_1, 'MMdd')
            ),
            'yyyyMMdd'
          )    
        END
      ELSE outbound_flight_date_1 
    END AS outbound_flight_date,
    
    * 
    
  FROM (
    SELECT 
      t.message,
      
      CASE
        WHEN t.message LIKE '%BPM%' THEN 'BPM'
        WHEN t.message LIKE '%BSM%' THEN 'BSM'
        ELSE 'OTHER'
      END AS message_type,
      
      CASE
        WHEN t.message LIKE '%BSM%' AND t.message LIKE '%CHG%' THEN 'CHG'
        WHEN t.message LIKE '%BSM%' AND t.message LIKE '%DEL%' THEN 'DEL'
        ELSE NULL
      END AS bsm_chg_del,
      
      regexp_extract(t.message, '\\.V/([^\r\n]+)', 1) AS `[.V/] Version and Supplementary Data`,
      regexp_extract(t.message, '\\.K/([^\r\n]+)', 1) AS `[.K/] Default Message Printer`,
      regexp_extract(t.message, '\\.J/([^\r\n]+)', 1) AS `[.J/] Processing Information`,
      regexp_extract(t.message, '\\.I/([^\r\n]+)', 1) AS `[.I/] Inbound Flight Information`,
      regexp_extract(t.message, '\\.F/([^\r\n]+)', 1) AS `[.F/] Outbound Flight Information`,
      regexp_extract(t.message, '\\.O/([^\r\n]+)', 1) AS `[.O/] Onward Flight Information`,
      
      split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[0] AS inbound_flight_number,
      split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1] AS inbound_flight_date_txt,
      
      -- Inbound flight date with single-digit day handling
      CASE
        WHEN right(split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1], 3) IN (
          'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
        )
        AND split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1] RLIKE '^[0-3]?[0-9][A-Z]{3}$'
        AND to_date(
          concat(
            CASE
              WHEN split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1] RLIKE '^[0-9][A-Z]{3}$' 
              THEN concat('0', split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1])
              ELSE split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1]
            END,
            date_format(t.eventts, 'yyyy')
          ),
          'ddMMMyyyy'
        ) IS NOT NULL 
        THEN to_date(
          concat(
            CASE
              WHEN split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1] RLIKE '^[0-9][A-Z]{3}$' 
              THEN concat('0', split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1])
              ELSE split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[1]
            END,
            date_format(t.eventts, 'yyyy')
          ),
          'ddMMMyyyy'
        )
        ELSE NULL
      END AS inbound_flight_date,
      
      split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[2] AS inbound_departure_airport,
      split(regexp_extract(t.message, '\\.I/([^\r\n]+)', 1), '/')[3] AS inbound_booking_class,
      
      split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[0] AS outbound_flight_number,
      split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[1] AS outbound_flight_date_txt,
      
      -- Outbound flight date with single-digit day handling
      CASE
        WHEN right(split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[1], 3) IN (
          'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
        ) 
        THEN to_date(
          concat(
            CASE
              WHEN split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[1] RLIKE '^[0-9][A-Z]{3}$' 
              THEN concat('0', split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[1])
              ELSE split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[1]
            END,
            date_format(t.eventts, 'yyyy')
          ),
          'ddMMMyyyy'
        )
        ELSE NULL
      END AS outbound_flight_date_1,
      
      split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[2] AS outbound_departure_airport,
      split(regexp_extract(t.message, '\\.F/([^\r\n]+)', 1), '/')[3] AS outbound_booking_class,
      
      split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[0] AS onward_flight_number,
      split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[1] AS onward_flight_date_txt,
      
      -- Onward flight date with single-digit day handling
      CASE
        WHEN right(split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[1], 3) IN (
          'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
        ) 
        THEN to_date(
          concat(
            CASE
              WHEN split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[1] RLIKE '^[0-9][A-Z]{3}$' 
              THEN concat('0', split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[1])
              ELSE split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[1]
            END,
            date_format(t.eventts, 'yyyy')
          ),
          'ddMMMyyyy'
        )
        ELSE NULL
      END AS onward_flight_date,
      
      split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[2] AS onward_departure_airport,
      split(regexp_extract(t.message, '\\.O/([^\r\n]+)', 1), '/')[3] AS onward_booking_class,
      
      regexp_extract(t.message, '\\.U/([^\r\n]+)', 1) AS `[.U/] Loading Data`,
      regexp_extract(t.message, '\\.N/([^\r\n]+)', 1) AS `[.N/] Baggage Tag Details`,
      regexp_extract(t.message, '\\.B/([^\r\n]+)', 1) AS `[.B/] Baggage Irregularities`,
      regexp_extract(t.message, '\\.Q/([^\r\n]+)', 1) AS `[.Q/] Load Sequence Number`,
      regexp_extract(t.message, '\\.S/([^\r\n]+)', 1) AS `[.S/] Reconciliation Data`,
      regexp_extract(t.message, '\\.P/([^\r\n]+)', 1) AS `[.P/] Passenger Name`,
      regexp_extract(t.message, '\\.Y/([^\r\n]+)', 1) AS `[.Y/] Frequent Traveller Number`,
      regexp_extract(t.message, '\\.C/([^\r\n]+)', 1) AS `[.C/] Corporate Or Group Name`,
      regexp_extract(t.message, '\\.L/([^\r\n]+)', 1) AS `[.L/] Automated PNR Address`,
      regexp_extract(t.message, '\\.E/([^\r\n]+)', 1) AS `[.E/] Baggage Exception Data`,
      regexp_extract(t.message, '\\.R/([^\r\n]+)', 1) AS `[.R/] Internal Airline Data`,
      regexp_extract(t.message, '\\.X/([^\r\n]+)', 1) AS `[.X/] Baggage Security Screening`,
      regexp_extract(t.message, '\\.T/([^\r\n]+)', 1) AS `[.T/] Baggage Tag Printer ID`,
      regexp_extract(t.message, '\\.A/([^\r\n]+)', 1) AS `[.A/] Baggage Routing Information`,
      regexp_extract(t.message, '\\.D/([^\r\n]+)', 1) AS `[.D/] Departure Airport / Date Info`,
      regexp_extract(t.message, '\\.M/([^\r\n]+)', 1) AS `[.M/] Manual Tag Indicator`,
      regexp_extract(t.message, '\\.H/([^\r\n]+)', 1) AS `[.H/] Handling Information`,
      regexp_extract(t.message, '\\.W/([^\r\n]+)', 1) AS `[.W/] Pieces And Weight Data`,
      
      left(regexp_extract(t.message, '\\.W/([^\r\n]+)', 1), 1) AS weight_first_c,
      
      CAST(
        NULLIF(split(regexp_extract(t.message, '\\.W/([^\r\n]+)', 1), '/')[1], '') 
        AS INT
      ) AS weight_pieces,
      
      CAST(
        NULLIF(split(regexp_extract(t.message, '\\.W/([^\r\n]+)', 1), '/')[2], '') 
        AS INT
      ) AS `weight(kg)`,
      
      t.idevent,
      t.key,
      t.eventts,
      t.insertts,
      t.eventtime,
      t.upsert_time,
      t.passive
      
    FROM vanderlande.iata_tmp t
  ) b 
) q2;
