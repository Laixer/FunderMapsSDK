-- Phase A5: Fix model_risk_manifest
--
-- Bug fix: external_building_id and neighborhood_id were missing from the
-- ON CONFLICT DO UPDATE SET list. This means redistricted buildings or
-- buildings with changed BAG IDs would keep stale values.
--
-- Design decision: No DELETE of stale rows — intentional. Maplayer views
-- filter via JOIN to building_active, so deactivated buildings don't appear
-- on maps. Statistics matviews that read model_risk_static directly still
-- include them — this is accepted behavior.

CREATE OR REPLACE PROCEDURE data.model_risk_manifest()
LANGUAGE sql
AS $$
INSERT INTO data.model_risk_static
SELECT
    building_id,
    external_building_id,
    address_count,
    neighborhood_id,
    construction_year,
    construction_year_reliability,
    foundation_type,
    foundation_type_reliability,
    restoration_costs,
    drystand,
    drystand_risk,
    drystand_risk_reliability,
    bio_infection_risk,
    bio_infection_risk_reliability,
    dewatering_depth,
    dewatering_depth_risk,
    dewatering_depth_risk_reliability,
    unclassified_risk,
    height,
    velocity,
    ground_water_level,
    ground_level,
    soil,
    surface_area,
    owner,
    inquiry_id,
    inquiry_type,
    damage_cause,
    enforcement_term,
    overall_quality,
    recovery_type
FROM data.model_risk_dynamic_all
ON CONFLICT (building_id) DO UPDATE SET
    external_building_id = excluded.external_building_id,
    neighborhood_id = excluded.neighborhood_id,
    address_count = excluded.address_count,
    construction_year = excluded.construction_year,
    construction_year_reliability = excluded.construction_year_reliability,
    foundation_type = excluded.foundation_type,
    foundation_type_reliability = excluded.foundation_type_reliability,
    restoration_costs = excluded.restoration_costs,
    drystand = excluded.drystand,
    drystand_risk = excluded.drystand_risk,
    drystand_risk_reliability = excluded.drystand_risk_reliability,
    bio_infection_risk = excluded.bio_infection_risk,
    bio_infection_risk_reliability = excluded.bio_infection_risk_reliability,
    dewatering_depth = excluded.dewatering_depth,
    dewatering_depth_risk = excluded.dewatering_depth_risk,
    dewatering_depth_risk_reliability = excluded.dewatering_depth_risk_reliability,
    unclassified_risk = excluded.unclassified_risk,
    height = excluded.height,
    velocity = excluded.velocity,
    ground_water_level = excluded.ground_water_level,
    ground_level = excluded.ground_level,
    soil = excluded.soil,
    surface_area = excluded.surface_area,
    owner = excluded.owner,
    inquiry_id = excluded.inquiry_id,
    inquiry_type = excluded.inquiry_type,
    damage_cause = excluded.damage_cause,
    enforcement_term = excluded.enforcement_term,
    overall_quality = excluded.overall_quality,
    recovery_type = excluded.recovery_type;
$$;
