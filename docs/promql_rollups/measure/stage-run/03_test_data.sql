-- m800_a, hour 0, H0 = 1794164400000, g = 0 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794164400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 0 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794168000000), toInt64(1794164400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 1, H0 = 1794168000000, g = 60 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794168000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 60 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794171600000), toInt64(1794168000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 2, H0 = 1794171600000, g = 120 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794171600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 120 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794175200000), toInt64(1794171600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 3, H0 = 1794175200000, g = 180 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794175200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 180 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794178800000), toInt64(1794175200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 4, H0 = 1794178800000, g = 240 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794178800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 240 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794182400000), toInt64(1794178800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 5, H0 = 1794182400000, g = 300 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794182400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 300 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794186000000), toInt64(1794182400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 6, H0 = 1794186000000, g = 360 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794186000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 360 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794189600000), toInt64(1794186000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 7, H0 = 1794189600000, g = 420 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794189600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 420 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794193200000), toInt64(1794189600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 8, H0 = 1794193200000, g = 480 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794193200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 480 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794196800000), toInt64(1794193200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 9, H0 = 1794196800000, g = 540 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794196800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 540 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794200400000), toInt64(1794196800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 10, H0 = 1794200400000, g = 600 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794200400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 600 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794204000000), toInt64(1794200400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 11, H0 = 1794204000000, g = 660 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794204000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 660 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794207600000), toInt64(1794204000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 12, H0 = 1794207600000, g = 720 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794207600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 720 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794211200000), toInt64(1794207600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 13, H0 = 1794211200000, g = 780 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794211200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 780 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794214800000), toInt64(1794211200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 14, H0 = 1794214800000, g = 840 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794214800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 840 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794218400000), toInt64(1794214800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 15, H0 = 1794218400000, g = 900 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794218400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 900 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794222000000), toInt64(1794218400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 16, H0 = 1794222000000, g = 960 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794222000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 960 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794225600000), toInt64(1794222000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 17, H0 = 1794225600000, g = 1020 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794225600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1020 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794229200000), toInt64(1794225600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 18, H0 = 1794229200000, g = 1080 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794229200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1080 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794232800000), toInt64(1794229200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 19, H0 = 1794232800000, g = 1140 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794232800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1140 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794236400000), toInt64(1794232800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 20, H0 = 1794236400000, g = 1200 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794236400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1200 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794240000000), toInt64(1794236400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 21, H0 = 1794240000000, g = 1260 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794240000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1260 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794243600000), toInt64(1794240000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 22, H0 = 1794243600000, g = 1320 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794243600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1320 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794247200000), toInt64(1794243600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, hour 23, H0 = 1794247200000, g = 1380 + m
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794247200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1380 + intDiv(number, 20000) AS g FROM numbers(20000 * 60))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794250800000), toInt64(1794247200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 0, H0 = 1794250800000, g = 1440
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794250800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1440 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794250860000), toInt64(1794250800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 1, H0 = 1794250860000, g = 1441
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794250860000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1441 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794250920000), toInt64(1794250860000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 2, H0 = 1794250920000, g = 1442
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794250920000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1442 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794250980000), toInt64(1794250920000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 3, H0 = 1794250980000, g = 1443
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794250980000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1443 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251040000), toInt64(1794250980000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 4, H0 = 1794251040000, g = 1444
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251040000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1444 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251100000), toInt64(1794251040000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 5, H0 = 1794251100000, g = 1445
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251100000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1445 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251160000), toInt64(1794251100000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 6, H0 = 1794251160000, g = 1446
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251160000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1446 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251220000), toInt64(1794251160000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 7, H0 = 1794251220000, g = 1447
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251220000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1447 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251280000), toInt64(1794251220000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 8, H0 = 1794251280000, g = 1448
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251280000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1448 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251340000), toInt64(1794251280000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 9, H0 = 1794251340000, g = 1449
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251340000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1449 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251400000), toInt64(1794251340000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 10, H0 = 1794251400000, g = 1450
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251400000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1450 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251460000), toInt64(1794251400000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 11, H0 = 1794251460000, g = 1451
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251460000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1451 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251520000), toInt64(1794251460000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 12, H0 = 1794251520000, g = 1452
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251520000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1452 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251580000), toInt64(1794251520000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 13, H0 = 1794251580000, g = 1453
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251580000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1453 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251640000), toInt64(1794251580000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 14, H0 = 1794251640000, g = 1454
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251640000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1454 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251700000), toInt64(1794251640000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 15, H0 = 1794251700000, g = 1455
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251700000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1455 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251760000), toInt64(1794251700000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 16, H0 = 1794251760000, g = 1456
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251760000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1456 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251820000), toInt64(1794251760000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 17, H0 = 1794251820000, g = 1457
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251820000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1457 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251880000), toInt64(1794251820000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 18, H0 = 1794251880000, g = 1458
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251880000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1458 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794251940000), toInt64(1794251880000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 19, H0 = 1794251940000, g = 1459
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794251940000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1459 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252000000), toInt64(1794251940000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 20, H0 = 1794252000000, g = 1460
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252000000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1460 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252060000), toInt64(1794252000000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 21, H0 = 1794252060000, g = 1461
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252060000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1461 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252120000), toInt64(1794252060000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 22, H0 = 1794252120000, g = 1462
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252120000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1462 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252180000), toInt64(1794252120000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 23, H0 = 1794252180000, g = 1463
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252180000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1463 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252240000), toInt64(1794252180000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 24, H0 = 1794252240000, g = 1464
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252240000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1464 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252300000), toInt64(1794252240000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 25, H0 = 1794252300000, g = 1465
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252300000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1465 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252360000), toInt64(1794252300000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 26, H0 = 1794252360000, g = 1466
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252360000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1466 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252420000), toInt64(1794252360000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 27, H0 = 1794252420000, g = 1467
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252420000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1467 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252480000), toInt64(1794252420000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 28, H0 = 1794252480000, g = 1468
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252480000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1468 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252540000), toInt64(1794252480000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 29, H0 = 1794252540000, g = 1469
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252540000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1469 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252600000), toInt64(1794252540000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 30, H0 = 1794252600000, g = 1470
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252600000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1470 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252660000), toInt64(1794252600000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 31, H0 = 1794252660000, g = 1471
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252660000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1471 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252720000), toInt64(1794252660000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 32, H0 = 1794252720000, g = 1472
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252720000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1472 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252780000), toInt64(1794252720000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 33, H0 = 1794252780000, g = 1473
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252780000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1473 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252840000), toInt64(1794252780000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 34, H0 = 1794252840000, g = 1474
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252840000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1474 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252900000), toInt64(1794252840000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 35, H0 = 1794252900000, g = 1475
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252900000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1475 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794252960000), toInt64(1794252900000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 36, H0 = 1794252960000, g = 1476
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794252960000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1476 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253020000), toInt64(1794252960000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 37, H0 = 1794253020000, g = 1477
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253020000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1477 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253080000), toInt64(1794253020000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 38, H0 = 1794253080000, g = 1478
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253080000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1478 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253140000), toInt64(1794253080000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 39, H0 = 1794253140000, g = 1479
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253140000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1479 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253200000), toInt64(1794253140000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 40, H0 = 1794253200000, g = 1480
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253200000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1480 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253260000), toInt64(1794253200000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 41, H0 = 1794253260000, g = 1481
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253260000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1481 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253320000), toInt64(1794253260000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 42, H0 = 1794253320000, g = 1482
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253320000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1482 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253380000), toInt64(1794253320000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 43, H0 = 1794253380000, g = 1483
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253380000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1483 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253440000), toInt64(1794253380000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 44, H0 = 1794253440000, g = 1484
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253440000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1484 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253500000), toInt64(1794253440000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 45, H0 = 1794253500000, g = 1485
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253500000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1485 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253560000), toInt64(1794253500000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 46, H0 = 1794253560000, g = 1486
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253560000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1486 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253620000), toInt64(1794253560000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 47, H0 = 1794253620000, g = 1487
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253620000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1487 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253680000), toInt64(1794253620000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 48, H0 = 1794253680000, g = 1488
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253680000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1488 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253740000), toInt64(1794253680000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 49, H0 = 1794253740000, g = 1489
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253740000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1489 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253800000), toInt64(1794253740000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 50, H0 = 1794253800000, g = 1490
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253800000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1490 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253860000), toInt64(1794253800000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 51, H0 = 1794253860000, g = 1491
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253860000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1491 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253920000), toInt64(1794253860000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 52, H0 = 1794253920000, g = 1492
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253920000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1492 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794253980000), toInt64(1794253920000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 53, H0 = 1794253980000, g = 1493
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794253980000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1493 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254040000), toInt64(1794253980000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 54, H0 = 1794254040000, g = 1494
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254040000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1494 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254100000), toInt64(1794254040000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 55, H0 = 1794254100000, g = 1495
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254100000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1495 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254160000), toInt64(1794254100000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 56, H0 = 1794254160000, g = 1496
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254160000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1496 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254220000), toInt64(1794254160000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 57, H0 = 1794254220000, g = 1497
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254220000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1497 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254280000), toInt64(1794254220000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 58, H0 = 1794254280000, g = 1498
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254280000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1498 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254340000), toInt64(1794254280000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);

-- m800_a, minute 59, H0 = 1794254340000, g = 1499
INSERT INTO m800_a.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at)
SELECT toUInt8(0), 'a', reinterpretAsUInt128(MD5(toString(s))), toInt64(1794254340000 + (m + 1) * 60000) AS te, te - 1000 * (s % 15) - 1,
  if(s % 2 = 0, s * 1000 + g * 4 * (1 + s % 7), (cityHash64(s, g) % 10000) / 100.) AS lv,
  if(s % 2 = 0, lv - 3 * (1 + s % 7), lv - (cityHash64(s, g, 1) % 100) / 100.),
  if(s % 2 = 0, lv, lv + (cityHash64(s, g, 3) % 100) / 100.),
  if(s % 2 = 0, 4 * lv - 6 * (1 + s % 7), 4 * lv), toUInt32(4),
  if(s % 2 = 0, 4. * (1 + s % 7), (cityHash64(s, g, 2) % 1000) / 10.), toUInt8(0), toInt64(1794261047702)
FROM (SELECT number % 20000 AS s, intDiv(number, 20000) AS m, 1499 + intDiv(number, 20000) AS g FROM numbers(20000 * 1))
UNION ALL
SELECT toUInt8(1), '', toUInt128(0), toInt64(1794254400000), toInt64(1794254340000), 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), toInt64(1794261047702);
