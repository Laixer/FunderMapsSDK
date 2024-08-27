INSERT INTO "data".subsidence
SELECT b.id, velocity
FROM public.panden AS p
JOIN geocoder.building AS b ON b.external_id = 'NL.IMBAG.PAND.' || p.identificatie
ON conflict ON CONSTRAINT subsidence_pkey DO nothing;
