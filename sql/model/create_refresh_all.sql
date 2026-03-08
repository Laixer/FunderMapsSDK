-- data.refresh_all() — Full model refresh procedure
--
-- Scheduled via pg_cron: daily at 18:00 UTC
-- Replaces the Python refresh_models.py + systemd timer for the model refresh.
--
-- Sequence:
--   1. Refresh sample matviews (building, cluster, supercluster)
--   2. Run risk model manifest (INSERT ON CONFLICT into model_risk_static)
--   3. Reindex model_risk_static
--   4. Refresh all 12 statistics matviews
--   5. Submit process_mapset job to worker queue (tile generation)

CREATE OR REPLACE PROCEDURE data.refresh_all()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Refresh sample matviews
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.building_sample;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.cluster_sample;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.supercluster_sample;

    -- Step 2: Run risk model manifest (INSERT ON CONFLICT into model_risk_static)
    CALL data.model_risk_manifest();

    -- Step 3: Reindex
    REINDEX TABLE data.model_risk_static;

    -- Step 4: Refresh statistics matviews
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_inquiries;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_inquiry_municipality;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_incidents;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_incident_municipality;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_foundation_type;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_foundation_risk;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_data_collected;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_construction_years;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_product_buildings_restored;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_postal_code_foundation_type;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_postal_code_foundation_risk;
    REFRESH MATERIALIZED VIEW CONCURRENTLY data.statistics_postal_code_data_collected;

    -- Step 5: Submit process_mapset job to worker queue
    INSERT INTO application.worker_jobs (job_type, status) VALUES ('process_mapset', 'pending');
END;
$$;
