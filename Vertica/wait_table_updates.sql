create table emre.table_status (
  table_name varchar(255),
  status varchar(50),
  row_count bigint,
  updated_at timestamp
);


insert into emre.table_status values ('emre.c','success','1',current_date)

call  emre.wait_for_table_updates(
    'emre.a,emre.c',
    current_date,      
    5,     
    20
);

-- Vertica Procedure: table_status tablosunda belirtilen tabloların güncellenip güncellenmediğini kontrol eder
-- Tüm tablolar güncellenene kadar bekler, timeout süresi dolarsa hata fırlatır

CREATE OR REPLACE PROCEDURE wait_for_table_updates(
    p_table_names VARCHAR,         -- Virgülle ayrılmış tablo adları (ör: 'tablo1,tablo2,tablo3')
    p_target_date DATE,            -- Kontrol edilecek güncelleme tarihi (ör: '2025-03-26')
    p_interval_seconds INT,        -- Kontrol arası bekleme süresi saniye cinsinden
    p_timeout_seconds INT          -- Maksimum bekleme süresi saniye cinsinden
)
LANGUAGE PLVSQL
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_timeout_time TIMESTAMP;
    v_check_count INT;
    v_table_count INT;
    v_remaining VARCHAR(5000);
    v_comma_pos INT;
    v_current_table VARCHAR(500);
    v_sleep_result INT;
    v_updated_tables VARCHAR(5000);
    v_missing_tables VARCHAR(5000);
    v_table_found INT;
    v_updated_list VARCHAR(5000);
    v_missing_list VARCHAR(5000);
BEGIN
    -- Başlangıç zamanını kaydet
    v_start_time := GETDATE();
    v_timeout_time := TIMESTAMPADD('second', p_timeout_seconds, v_start_time);
    
    -- Toplam tablo sayısını hesapla
    v_table_count := LENGTH(p_table_names) - LENGTH(REPLACE(p_table_names, ',', '')) + 1;
    
    -- Ana kontrol döngüsü
    LOOP
        v_check_count := 0;
        v_remaining := p_table_names || ',';
        v_updated_tables := '';
        v_missing_tables := '';
        
        -- Her tabloyu kontrol et
        LOOP
            v_comma_pos := POSITION(',' IN v_remaining);
            
            IF v_comma_pos <= 0 THEN
                EXIT;
            END IF;
            
            v_current_table := TRIM(SUBSTR(v_remaining, 1, v_comma_pos - 1));
            v_remaining := SUBSTR(v_remaining, v_comma_pos + 1);
            
            -- Tablo table_status'ta bu tarih için var mı?
            SELECT COUNT(*)
            INTO v_table_found
            FROM emre.table_status 
            WHERE LOWER(TRIM(table_name)) = LOWER(v_current_table)
              AND CAST(updated_at AS DATE) = p_target_date;
            
            IF v_table_found > 0 THEN
                v_check_count := v_check_count + 1;
                IF v_updated_tables = '' THEN
                    v_updated_tables := v_current_table;
                ELSE
                    v_updated_tables := v_updated_tables || ', ' || v_current_table;
                END IF;
            ELSE
                IF v_missing_tables = '' THEN
                    v_missing_tables := v_current_table;
                ELSE
                    v_missing_tables := v_missing_tables || ', ' || v_current_table;
                END IF;
            END IF;
        END LOOP;
        
        -- Tüm tablolar güncellendi mi?
        IF v_check_count >= v_table_count THEN
            -- SUCCESS
            RETURN;
        END IF;
        
        -- Timeout kontrolü
        IF GETDATE() >= v_timeout_time THEN
            -- TIMEOUT - Error fırlat ve detaylı bilgi ver
            IF v_updated_tables = '' THEN
                v_updated_list := 'YOK';
            ELSE
                v_updated_list := v_updated_tables;
            END IF;
            
            IF v_missing_tables = '' THEN
                v_missing_list := 'YOK';
            ELSE
                v_missing_list := v_missing_tables;
            END IF;
            
            RAISE EXCEPTION 'Timeout: % saniye içinde tüm tablolar güncellenmedi. Güncellenmiş tablolar: [%]. Güncellenmemiş tablolar: [%]', 
                p_timeout_seconds, v_updated_list, v_missing_list;
        END IF;
        
        -- Belirtilen süre kadar bekle
        SELECT SLEEP(p_interval_seconds) INTO v_sleep_result;
    END LOOP;
END;
$$;

-- Kullanım:
-- CALL wait_for_table_updates('tablo1,tablo2,tablo3', '2025-03-26', 30, 300);
-- Her 30 saniyede kontrol eder, maksimum 300 saniye (5 dakika) bekler
