drop table if exists emre.table_status ;
create table emre.table_status (
  table_name varchar(255),
  row_count bigint,
  updated_at timestamp
);

--add log procedure merkezi yönetmek için 
CREATE OR REPLACE PROCEDURE emre.log_table_status(
    p_table_name VARCHAR            -- Log'u atılacak tablo adı (schema.table formatında olabilir)
)
LANGUAGE PLVSQL
AS $$
DECLARE
    v_row_count BIGINT;
    v_sql VARCHAR(5000);
BEGIN
    -- Tablonun row count'unu hesapla
    v_row_count  := execute 'SELECT COUNT(*) FROM ' || p_table_name;

    -- table_status tablosuna kayıt ekle
    execute 'INSERT INTO emre.table_status (table_name, row_count, updated_at)
    VALUES ('''||p_table_name||''','|| v_row_count||', GETDATE()) ;    COMMIT; ';
    

END;
$$;



-- Vertica Procedure: table_status tablosunda belirtilen tabloların güncellenip güncellenmediğini kontrol eder
-- Tüm tablolar güncellenene kadar bekler, timeout süresi dolarsa hata fırlatır

CREATE OR REPLACE PROCEDURE emre.wait_for_table_updates(
    p_table_names VARCHAR,         -- Virgülle ayrılmış tablo adları (ör: 'tablo1,tablo2,tablo3')
    p_target_date DATE,            -- Kontrol edilecek güncelleme tarihi (ör: '2025-03-26')
    p_interval_seconds INT,        -- Kontrol arası bekleme süresi saniye cinsinden
    p_timeout_seconds INT,         -- Maksimum bekleme süresi saniye cinsinden
    p_check_row_counts BOOLEAN     -- TRUE: row_count > 0 kontrolü yap, FALSE: sadece updated_at kontrol et
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
    v_table_row_count BIGINT;
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
            
            -- Tablo table_status'ta bu tarih için var mı ve row_count bilgisi
            SELECT COUNT(*), COALESCE(MAX(row_count), 0)
            INTO v_table_found, v_table_row_count
            FROM emre.table_status 
            WHERE LOWER(TRIM(table_name)) = LOWER(v_current_table)
              AND CAST(updated_at AS DATE) = p_target_date;
            
            -- Kontrol mantığı
            IF v_table_found = 0 THEN
                -- Kayıt yok - updated_at güncel değil
                IF v_missing_tables = '' THEN
                    v_missing_tables := v_current_table || ' (ilgili gün için kayit yok)';
                ELSE
                    v_missing_tables := v_missing_tables || ', ' || v_current_table || ' (ilgili gün için kayit yok)';
                END IF;
            ELSIF p_check_row_counts = TRUE AND v_table_row_count = 0 THEN
                -- Kayıt var ama row_count = 0 (sadece check_row_counts=TRUE ise)
                IF v_missing_tables = '' THEN
                    v_missing_tables := v_current_table || ' (row_count=0)';
                ELSE
                    v_missing_tables := v_missing_tables || ', ' || v_current_table || ' (row_count=0)';
                END IF;
            ELSE
                -- Başarılı: Kayıt var ve row_count kontrolü geçti (veya kontrol edilmedi)
                v_check_count := v_check_count + 1;
                IF v_updated_tables = '' THEN
                    v_updated_tables := v_current_table;
                ELSE
                    v_updated_tables := v_updated_tables || ', ' || v_current_table;
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
            
            RAISE EXCEPTION 'Timeout: % saniye icinde tum tablolar guncellenmedi. Guncel tablolar: [%]. Guncel olmayan tablolar: [%]', 
                p_timeout_seconds, v_updated_list, v_missing_list;
        END IF;
        
        -- Belirtilen süre kadar bekle
        SELECT SLEEP(p_interval_seconds) INTO v_sleep_result;
    END LOOP;
END;
$$;

-- Kullanım Örnekleri:

-- 1. Sadece updated_at kontrolü (row_count kontrolü YOK)
-- CALL wait_for_table_updates('tablo1,tablo2,tablo3', '2025-03-26', 30, 300, FALSE);

-- 2. Hem updated_at hem row_count > 0 kontrolü
-- CALL wait_for_table_updates('tablo1,tablo2,tablo3', '2025-03-26', 30, 300, TRUE);

-- 3. Örnek timeout mesajı:
--    Güncel olmayan tablolar: [tablo1 (kayit yok), tablo2 (row_count=0)]
--    - "kayit yok": table_status tablosunda bu tarih için kayıt bulunamadı
--    - "row_count=0": Kayıt var ama satır sayısı 0 (check_row_counts=TRUE olmalı)
