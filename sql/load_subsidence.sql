INSERT INTO "data".building_subsidence
SELECT b.external_id, p.velocity
FROM public.panden AS p
JOIN geocoder.building AS b ON b.external_id = 'NL.IMBAG.PAND.' || p.identificatie
WHERE p.velocity is not NULL
ON conflict ON CONSTRAINT building_subsidence_pkey DO nothing;
