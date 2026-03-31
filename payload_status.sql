UPDATE emails
SET payload_status = 'validated'
WHERE json_payload IS NOT NULL;

--Old script
UPDATE emails
SET payload_status = 'validated'
WHERE json_payload IS NOT NULL
  AND json_payload::text ILIKE '%fileBinary%'  -- or adjusted key
  AND LENGTH(json_payload::text) > 1000
  AND payload_status = 'pending';
