INSERT INTO "data".building_subsidence
SELECT b.external_id, velocity
FROM public.panden AS p
JOIN geocoder.building AS b ON b.external_id = 'NL.IMBAG.PAND.' || p.identificatie
ON conflict ON CONSTRAINT building_subsidence_pkey DO nothing;
