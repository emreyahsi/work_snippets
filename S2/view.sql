
SELECT
    t.message,
    CASE
        WHEN t.message ~~ '%BPM%' :: text THEN 'BPM' :: text
        WHEN t.message ~~ '%BSM%' :: text THEN 'BSM' :: text
        ELSE 'OTHER' :: text
    END AS message_type,
    CASE
        WHEN t.message ~~ '%BSM%' :: text
        AND t.message ~~ '%CHG%' :: text THEN 'CHG' :: text
        WHEN t.message ~~ '%BSM%' :: text
        AND t.message ~~ '%DEL%' :: text THEN 'DEL' :: text
        ELSE NULL :: text
    END AS bsm_chg_del,
    "substring"(t.message, '\.V/([^\r\n]+)' :: text) AS "[.V/] Version and Supplementary Data",
    "substring"(t.message, '\.K/([^\r\n]+)' :: text) AS "[.K/] Default Message Printer",
    "substring"(t.message, '\.J/([^\r\n]+)' :: text) AS "[.J/] Processing Information",
    "substring"(t.message, '\.I/([^\r\n]+)' :: text) AS "[.I/] Inbound Flight Information",
    "substring"(t.message, '\.F/([^\r\n]+)' :: text) AS "[.F/] Outbound Flight Information",
    "substring"(t.message, '\.O/([^\r\n]+)' :: text) AS "[.O/] Onward Flight Information",
    split_part(
        "substring"(t.message, '\.I/([^\r\n]+)' :: text),
        '/' :: text,
        1
    ) AS inbound_flight_number,
    split_part(
        "substring"(t.message, '\.I/([^\r\n]+)' :: text),
        '/' :: text,
        2
    ) AS inbound_flight_date_txt,
    CASE
        WHEN (
            "right"(
                split_part(
                    "substring"(t.message, '\.I/([^\r\n]+)' :: text),
                    '/' :: text,
                    2
                ),
                3
            ) = ANY (
                ARRAY ['JAN'::text, 'FEB'::text, 'MAR'::text, 'APR'::text, 'MAY'::text,                 'JUN'::text, 'JUL'::text, 'AUG'::text, 'SEP'::text, 'OCT'::text, 'NOV'::text,                 'DEC'::text]
            )
        )
        AND split_part(
            "substring"(t.message, '\.I/([^\r\n]+)' :: text),
            '/' :: text,
            2
        ) ~ '^[0-3][0-9][A-Z]{3}$' :: text
        AND util.to_date_safe(
            split_part(
                "substring"(t.message, '\.I/([^\r\n]+)' :: text),
                '/' :: text,
                2
            ) || TO_CHAR(t.eventts, 'YYYY' :: text),
            'DDMONYYYY' :: text
        ) IS NOT NULL THEN util.to_date_safe(
            split_part(
                "substring"(t.message, '\.I/([^\r\n]+)' :: text),
                '/': :text,
                2
            ) || TO_CHAR(t.eventts, 'YYYY' :: text),
            'DDMONYYYY' :: text
        )
        ELSE NULL :: date
    END AS inbound_flight_date,
    split_part(
        "substring"(t.message, '\.I/([^\r\n]+)' :: text),
        '/' :: text,
        3
    ) AS inbound_departure_airport,
    split_part(
        "substring"(t.message, '\.I/([^\r\n]+)' :: text),
        '/' :: text,
        4
    ) AS inbound_booking_class,
    split_part(
        "substring"(t.message, '\.F/([^\r\n]+)' :: text),
        '/' :: text,
        1
    ) AS outbound_flight_number,
    split_part(
        "substring"(t.message, '\.F/([^\r\n]+)' :: text),
        '/' :: text,
        2
    ) AS outbound_flight_date_txt,
    CASE
        WHEN "right"(
            split_part(
                "substring"(t.message, '\.F/([^\r\n]+)' :: text),
                '/' :: text,
                2
            ),
            3
        ) = ANY (
            ARRAY ['JAN'::text, 'FEB'::text, 'MAR'::text, 'APR'::text, 'MAY'::text, 'JUN':             :text, 'JUL'::text, 'AUG'::text, 'SEP'::text, 'OCT'::text, 'NOV'::text, 'DEC'::text]
        ) THEN util.to_date_safe(
            split_part(
                "substring"(t.message, '\.F/([^\r\n]+)' :: text),
                '/': :text,
                2
            ) || TO_CHAR(t.eventts, 'YYYY' :: text),
            'DDMONYYYY' :: text
        )
        ELSE NULL :: date
    END AS outbound_flight_date,
    split_part(
        "substring"(t.message, '\.F/([^\r\n]+)' :: text),
        '/' :: text,
        3
    ) AS outbound_departure_airport,
    split_part(
        "substring"(t.message, '\.F/([^\r\n]+)' :: text),
        '/' :: text,
        4
    ) AS outbound_booking_class,
    split_part(
        "substring"(t.message, '\.O/([^\r\n]+)' :: text),
        '/' :: text,
        1
    ) AS onward_flight_number,
    split_part(
        "substring"(t.message, '\.O/([^\r\n]+)' :: text),
        '/' :: text,
        2
    ) AS onward_flight_date_txt,
    CASE
        WHEN "right"(
            split_part(
                "substring"(t.message, '\.O/([^\r\n]+)' :: text),
                '/' :: text,
                2
            ),
            3
        ) = ANY (
            ARRAY ['JAN'::text, 'FEB'::text, 'MAR'::text, 'APR'::text, 'MAY'::text, 'JUN':             :text, 'JUL'::text, 'AUG'::text, 'SEP'::text, 'OCT'::text, 'NOV'::text, 'DEC'::text]
        ) THEN util.to_date_safe(
            split_part(
                "substring"(t.message, '\.O/([^\r\n]+)' :: text),
                '/': :text,
                2
            ) || TO_CHAR(t.eventts, 'YYYY' :: text),
            'DDMONYYYY' :: text
        )
        ELSE NULL :: date
    END AS onward_flight_date,
    split_part(
        "substring"(t.message, '\.O/([^\r\n]+)' :: text),
        '/' :: text,
        3
    ) AS onward_departure_airport,
    split_part(
        "substring"(t.message, '\.O/([^\r\n]+)' :: text),
        '/' :: text,
        4
    ) AS onward_booking_class,
    "substring"(t.message, '\.U/([^\r\n]+)' :: text) AS "[.U/] Loading Data",
    "substring"(t.message, '\.N/([^\r\n]+)' :: text) AS "[.N/] Baggage Tag Details",
    "substring"(t.message, '\.B/([^\r\n]+)' :: text) AS "[.B/] Baggage Irregularities",
    "substring"(t.message, '\.Q/([^\r\n]+)' :: text) AS "[.Q/] Load Sequence Number",
    "substring"(t.message, '\.S/([^\r\n]+)' :: text) AS "[.S/] Reconciliation Data",
    "substring"(t.message, '\.P/([^\r\n]+)' :: text) AS "[.P/] Passenger Name",
    "substring"(t.message, '\.Y/([^\r\n]+)' :: text) AS "[.Y/] Frequent Traveller Number",
    "substring"(t.message, '\.C/([^\r\n]+)' :: text) AS "[.C/] Corporate Or Group Name",
    "substring"(t.message, '\.L/([^\r\n]+)' :: text) AS "[.L/] Automated PNR Address",
    "substring"(t.message, '\.E/([^\r\n]+)' :: text) AS "[.E/] Baggage Exception Data",
    "substring"(t.message, '\.R/([^\r\n]+)' :: text) AS "[.R/] Internal Airline Data",
    "substring"(t.message, '\.X/([^\r\n]+)' :: text) AS "[.X/] Baggage Security Screening",
    "substring"(t.message, '\.T/([^\r\n]+)' :: text) AS "[.T/] Baggage Tag Printer ID",
    "substring"(t.message, '\.A/([^\r\n]+)' :: text) AS "[.A/] Baggage Routing Information ",
    "substring"(t.message, '\.D/([^\r\n]+)' :: text) AS "[.D/] Departure Airport / Date Info",
    "substring"(t.message, '\.M/([^\r\n]+)' :: text) AS "[.M/] Manual Tag Indicator",
    "substring"(t.message, '\.H/([^\r\n]+)' :: text) AS "[.H/] Handling Information",
    "substring"(t.message, '\.W/([^\r\n]+)' :: text) AS "[.W/] Pieces And Weight Data",
    "left"(
        "substring"(t.message, '\.W/([^\r\n]+)' :: text),
        1
    ) AS weight_first_c,
    NULLIF(
        split_part(
            "substring"(t.message, '\.W/([^\r\n]+)' :: text),
            '/' :: text,
            2
        ),
        '' :: text
    ): :integer AS weight_pieces,
    NULLIF(
        split_part(
            "substring"(t.message, '\.W/([^\r\n]+)' :: text),
            '/' :: text,
            3
        ),
        '' :: text
    ): :integer AS "weight(kg)",
    t.idevent,
    t.key,
    t.eventts,
    t.insertts,
    t.eventtime,
    t.upsert_time,
    t.passiveFROM wc_iata t;

