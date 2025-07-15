CREATE OR REPLACE VIEW data.model_risk_dynamic_all
 AS
 SELECT ba.id AS building_id,
    ba.external_id AS external_building_id,
    addresses.count AS address_count,
    ba.neighborhood_id,
    construction_year.construction_year,
        CASE
            WHEN established.built_year IS NOT NULL THEN 'established'::data.reliability
            ELSE 'indicative'::data.reliability
        END AS construction_year_reliability,
    foundation_type.foundation_type,
        CASE
            WHEN established.foundation_type IS NOT NULL THEN 'established'::data.reliability
            WHEN cluster.foundation_type IS NOT NULL THEN 'cluster'::data.reliability
            WHEN supercluster.foundation_type IS NOT NULL THEN 'supercluster'::data.reliability
            ELSE 'indicative'::data.reliability
        END AS foundation_type_reliability,
        CASE
            WHEN foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type THEN round((surface_area.surface_area::double precision * 1950::double precision)::numeric, '-2'::integer)::integer
            WHEN foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type THEN round((surface_area.surface_area::double precision * 350::double precision)::numeric, '-2'::integer)::integer
            ELSE NULL::integer
        END AS restoration_costs,
        CASE
            WHEN established.wood_level IS NOT NULL AND established.groundwater_level IS NOT NULL THEN (established.wood_level::numeric - established.groundwater_level::numeric)::double precision
            WHEN cluster.wood_level IS NOT NULL AND cluster.groundwater_level IS NOT NULL THEN (cluster.wood_level::numeric - cluster.groundwater_level::numeric)::double precision
            ELSE
            CASE
                WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type THEN gwl.level - 2.5::double precision
                WHEN foundation_type.foundation_type = 'wood'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type THEN gwl.level - 1.5::double precision
                WHEN foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type THEN gwl.level - 1.5::double precision
                ELSE NULL::double precision
            END
        END AS drystand,
    COALESCE(established_drystand_risk.established_drystand_risk, cluster_drystand_risk.cluster_drystand_risk, drystand_risk.drystand_risk) AS drystand_risk,
        CASE
            WHEN established_drystand_risk.established_drystand_risk IS NOT NULL THEN 'established'::data.reliability
            WHEN cluster_drystand_risk.cluster_drystand_risk IS NOT NULL THEN 'cluster'::data.reliability
            ELSE 'indicative'::data.reliability
        END AS drystand_risk_reliability,
    COALESCE(established_bio_infection_risk.established_bio_infection_risk, cluster_bio_infection_risk.cluster_bio_infection_risk, bio_infection_risk.bio_infection_risk) AS bio_infection_risk,
        CASE
            WHEN established_bio_infection_risk.established_bio_infection_risk IS NOT NULL THEN 'established'::data.reliability
            WHEN cluster_bio_infection_risk.cluster_bio_infection_risk IS NOT NULL THEN 'cluster'::data.reliability
            ELSE 'indicative'::data.reliability
        END AS bio_infection_risk_reliability,
        CASE
            WHEN established.foundation_depth IS NOT NULL AND established.groundwater_level IS NOT NULL THEN (established.foundation_depth::numeric - established.groundwater_level::numeric - 0.6)::double precision
            WHEN cluster.wood_level IS NOT NULL AND cluster.groundwater_level IS NOT NULL THEN (cluster.foundation_depth::numeric - cluster.groundwater_level::numeric - 0.6)::double precision
            ELSE
            CASE
                WHEN foundation_type.foundation_type = 'no_pile'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type THEN gwl.level - 0.6::double precision
                WHEN foundation_type.foundation_type = 'no_pile_bearing_floor'::report.foundation_type THEN gwl.level - 0.6::double precision
                ELSE NULL::double precision
            END
        END AS dewatering_depth,
    COALESCE(established_dewatering_depth_risk.established_dewatering_depth_risk, cluster_dewatering_depth_risk.cluster_dewatering_depth_risk, dewatering_depth_risk.dewatering_depth_risk) AS dewatering_depth_risk,
        CASE
            WHEN established_dewatering_depth_risk.established_dewatering_depth_risk IS NOT NULL THEN 'established'::data.reliability
            WHEN cluster_dewatering_depth_risk.cluster_dewatering_depth_risk IS NOT NULL THEN 'cluster'::data.reliability
            ELSE 'indicative'::data.reliability
        END AS dewatering_depth_risk_reliability,
    COALESCE(established_unclassified_risk.established_unclassified_risk, cluster_unclassified_risk.cluster_unclassified_risk) AS unclassified_risk,
    round(GREATEST(bh.height, 0::real)::numeric, 2) AS height,
    round(bs.velocity::numeric, 2) AS velocity,
    round(gwl.level::numeric, 2) AS ground_water_level,
    round(be.ground::numeric, 2) AS ground_level,
    gr.code AS soil,
    surface_area.surface_area,
    bo.owner,
    COALESCE(established.id, cluster.id) AS inquiry_id,
    COALESCE(established.inquiry_type, cluster.inquiry_type) AS inquiry_type,
    COALESCE(established.damage_cause, cluster.damage_cause) AS damage_cause,
    date_part('years'::text, age(
        CASE COALESCE(established.enforcement_term, cluster.enforcement_term)
            WHEN 'term05'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '5 years'::interval
            WHEN 'term510'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '10 years'::interval
            WHEN 'term1020'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '20 years'::interval
            WHEN 'term5'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '5 years'::interval
            WHEN 'term10'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '10 years'::interval
            WHEN 'term15'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '15 years'::interval
            WHEN 'term20'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '20 years'::interval
            WHEN 'term25'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '25 years'::interval
            WHEN 'term30'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '30 years'::interval
            WHEN 'term40'::report.enforcement_term THEN COALESCE(established.document_date, cluster.document_date) + '40 years'::interval
            ELSE NULL::timestamp without time zone
        END::timestamp with time zone, CURRENT_TIMESTAMP)) AS enforcement_term,
    COALESCE(established.overall_quality, cluster.overall_quality) AS overall_quality,
    recovery.type AS recovery_type
   FROM geocoder.building_active ba
     LEFT JOIN data.building_elevation be ON be.building_id = ba.external_id
     LEFT JOIN data.building_height bh ON bh.building_id = ba.external_id
     LEFT JOIN data.building_geographic_region gr ON gr.building_id = ba.external_id
     LEFT JOIN data.building_groundwater_level gwl ON gwl.building_id = ba.external_id
     LEFT JOIN data.building_subsidence bs ON bs.building_id = ba.external_id
     LEFT JOIN data.building_ownership bo ON bo.building_id = ba.external_id
     LEFT JOIN data.building_pleistocene bp ON bp.building_id = ba.external_id
     LEFT JOIN data.building_cluster bc ON bc.building_id = ba.external_id
     LEFT JOIN data.supercluster bsc ON bsc.cluster_id = bc.cluster_id
     LEFT JOIN data.building_sample established ON established.building_id = ba.external_id
     LEFT JOIN data.cluster_sample cluster ON cluster.cluster_id = bc.cluster_id
     LEFT JOIN data.supercluster_sample supercluster ON supercluster.supercluster_id = bsc.supercluster_id
     LEFT JOIN ( SELECT DISTINCT ON (rs.building_id) rs.building_id,
            rs.type
           FROM report.recovery_sample rs
          ORDER BY rs.building_id, rs.create_date DESC) recovery ON recovery.building_id = ba.external_id
     LEFT JOIN data.cluster_recovery_sample cluster_recovery_sample ON cluster_recovery_sample.cluster_id = bc.cluster_id,
    LATERAL ( SELECT COALESCE(established.built_year::integer, date_part('year'::text, ba.built_year::date)::integer) AS "coalesce") construction_year(construction_year),
    LATERAL round(st_area(ba.geom::geography, true)::numeric, 2) surface_area(surface_area),
    LATERAL round((be.ground - bp.depth)::numeric, 2) pile_length(pile_length),
    LATERAL ( SELECT count(a_1.id)::integer AS count
           FROM geocoder.address a_1
          WHERE a_1.building_id::text = ba.id::text) addresses(count),
    LATERAL ( SELECT
                CASE
                    WHEN construction_year.construction_year >= 1940 AND construction_year.construction_year < 1965 AND addresses.count >= 8 THEN 'concrete'::report.foundation_type
                    WHEN construction_year.construction_year >= 1965 AND (bh.height < 14::double precision OR bh.height IS NULL) AND (gr.code = 'hz'::text OR gr.code = 'ni-hz'::text OR gr.code = 'ni-du'::text) THEN 'no_pile'::report.foundation_type
                    WHEN construction_year.construction_year >= 1965 AND (bh.height < 14::double precision OR bh.height IS NULL) AND (gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'concrete'::report.foundation_type
                    WHEN construction_year.construction_year >= 1965 AND bh.height >= 14::double precision THEN 'concrete'::report.foundation_type
                    WHEN construction_year.construction_year >= 1700 AND construction_year.construction_year < 1800 AND (bh.height < 14::double precision OR bh.height IS NULL) AND (gr.code = 'hz'::text OR gr.code = 'ni-hz'::text OR gr.code = 'ni-du'::text) THEN 'no_pile'::report.foundation_type
                    WHEN construction_year.construction_year >= 1700 AND construction_year.construction_year < 1800 AND bh.height >= 14::double precision AND (gr.code = 'hz'::text OR gr.code = 'ni-hz'::text OR gr.code = 'ni-du'::text) THEN 'wood'::report.foundation_type
                    WHEN construction_year.construction_year >= 1700 AND construction_year.construction_year < 1800 AND bh.height < 8.5::double precision AND (gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'no_pile'::report.foundation_type
                    WHEN construction_year.construction_year >= 1700 AND construction_year.construction_year < 1800 AND (bh.height >= 8.5::double precision OR bh.height IS NULL) AND (gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'wood'::report.foundation_type
                    WHEN construction_year.construction_year >= 1800 AND construction_year.construction_year < 1965 AND (bh.height < 14::double precision OR bh.height IS NULL) AND (gr.code = 'hz'::text OR gr.code = 'ni-hz'::text OR gr.code = 'ni-du'::text) THEN 'no_pile'::report.foundation_type
                    WHEN construction_year.construction_year >= 1800 AND construction_year.construction_year < 1965 AND bh.height >= 14::double precision AND (gr.code = 'hz'::text OR gr.code = 'ni-hz'::text OR gr.code = 'ni-du'::text) THEN 'wood'::report.foundation_type
                    WHEN construction_year.construction_year >= 1800 AND construction_year.construction_year < 1965 AND bh.height < 8.5::double precision AND gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND (gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'no_pile'::report.foundation_type
                    WHEN construction_year.construction_year >= 1800 AND construction_year.construction_year < 1920 AND (bh.height >= 8.5::double precision OR bh.height IS NULL) AND (gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'wood'::report.foundation_type
                    WHEN construction_year.construction_year >= 1920 AND construction_year.construction_year < 1965 AND (bh.height >= 8.5::double precision OR bh.height IS NULL) AND (gr.code <> 'hz'::text AND gr.code <> 'ni-hz'::text AND gr.code <> 'ni-du'::text OR gr.code IS NULL) THEN 'wood_charger'::report.foundation_type
                    WHEN construction_year.construction_year < 1700 THEN 'no_pile'::report.foundation_type
                    WHEN bh.height >= 10.5::double precision THEN 'wood'::report.foundation_type
                    WHEN bh.height < 10.5::double precision THEN 'no_pile'::report.foundation_type
                    ELSE 'other'::report.foundation_type
                END AS "case") indicative_foundation_type(foundation_type),
    LATERAL ( SELECT COALESCE(established.foundation_type, cluster.foundation_type, supercluster.foundation_type, indicative_foundation_type.foundation_type) AS "coalesce") foundation_type(foundation_type),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'concrete'::report.foundation_type OR foundation_type.foundation_type = 'weighted_pile'::report.foundation_type THEN 'a'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity IS NULL AND gwl.level >= 1.5::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity IS NULL AND gwl.level < 1.5::double precision THEN 'b'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity < '-2.0'::numeric::double precision AND gwl.level >= 1.5::double precision THEN 'e'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity >= '-2.0'::numeric::double precision AND gwl.level >= 1.5::double precision THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity < '-2.0'::numeric::double precision AND gwl.level < 1.5::double precision THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND bs.velocity >= '-2.0'::numeric::double precision AND gwl.level < 1.5::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity IS NULL AND gwl.level >= 2.5::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity IS NULL AND gwl.level < 2.5::double precision THEN 'b'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity < '-1.0'::numeric::double precision AND gwl.level >= 2.5::double precision THEN 'e'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity < '-1.0'::numeric::double precision AND gwl.level < 2.5::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity >= '-1.0'::numeric::double precision AND gwl.level >= 2.5::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'wood_charger'::report.foundation_type AND bs.velocity >= '-1.0'::numeric::double precision AND gwl.level < 2.5::double precision THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") drystand_risk(drystand_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN foundation_type.foundation_type = 'concrete'::report.foundation_type OR foundation_type.foundation_type = 'weighted_pile'::report.foundation_type THEN 'a'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity IS NULL AND gwl.level < 0.6::double precision THEN 'c'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity IS NULL AND gwl.level >= 0.6::double precision THEN 'b'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity < '-1.0'::numeric::double precision AND gwl.level < 0.6::double precision THEN 'e'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity < '-1.0'::numeric::double precision AND gwl.level >= 0.6::double precision THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity >= '-1.0'::numeric::double precision AND gwl.level < 0.6::double precision THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'no_pile'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_masonry'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_strips'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_concrete_floor'::report.foundation_type OR foundation_type.foundation_type = 'no_pile_slit'::report.foundation_type) AND bs.velocity >= '-1.0'::numeric::double precision AND gwl.level >= 0.6::double precision THEN 'c'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") dewatering_depth_risk(dewatering_depth_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length <= 12::numeric AND bs.velocity < '-2.0'::numeric::double precision THEN 'e'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length <= 12::numeric THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length > 12::numeric AND pile_length.pile_length <= 15::numeric AND bs.velocity < '-2.0'::numeric::double precision THEN 'e'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length > 12::numeric AND pile_length.pile_length <= 15::numeric THEN 'c'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length > 15::numeric AND bs.velocity < '-2.0'::numeric::double precision THEN 'd'::data.foundation_risk_indication
                    WHEN (foundation_type.foundation_type = 'wood'::report.foundation_type OR foundation_type.foundation_type = 'wood_charger'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_amsterdam'::report.foundation_type OR foundation_type.foundation_type = 'wood_amsterdam_arch'::report.foundation_type OR foundation_type.foundation_type = 'wood_rotterdam_arch'::report.foundation_type) AND pile_length.pile_length > 15::numeric THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") bio_infection_risk(bio_infection_risk),
    LATERAL ( SELECT
                CASE
                    WHEN (cluster.damage_cause = 'drystand'::report.foundation_damage_cause OR cluster.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR cluster.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (cluster.enforcement_term = 'term05'::report.enforcement_term OR cluster.enforcement_term = 'term5'::report.enforcement_term OR cluster.recovery_advised OR cluster.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN (cluster.damage_cause = 'drystand'::report.foundation_damage_cause OR cluster.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR cluster.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (cluster.enforcement_term = 'term510'::report.enforcement_term OR cluster.enforcement_term = 'term10'::report.enforcement_term OR cluster.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN (cluster.damage_cause = 'drystand'::report.foundation_damage_cause OR cluster.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR cluster.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (cluster.enforcement_term = 'term1020'::report.enforcement_term OR cluster.enforcement_term = 'term15'::report.enforcement_term OR cluster.enforcement_term = 'term20'::report.enforcement_term OR cluster.overall_quality = 'mediocre'::report.foundation_quality OR cluster.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN (cluster.damage_cause = 'drystand'::report.foundation_damage_cause OR cluster.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR cluster.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (cluster.enforcement_term = 'term25'::report.enforcement_term OR cluster.enforcement_term = 'term30'::report.enforcement_term OR cluster.enforcement_term = 'term40'::report.enforcement_term OR cluster.overall_quality = 'good'::report.foundation_quality OR cluster.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") cluster_drystand_risk(cluster_drystand_risk),
    LATERAL ( SELECT
                CASE
                    WHEN cluster.damage_cause = 'drainage'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term05'::report.enforcement_term OR cluster.enforcement_term = 'term5'::report.enforcement_term OR cluster.recovery_advised OR cluster.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'drainage'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term510'::report.enforcement_term OR cluster.enforcement_term = 'term10'::report.enforcement_term OR cluster.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'drainage'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term1020'::report.enforcement_term OR cluster.enforcement_term = 'term15'::report.enforcement_term OR cluster.enforcement_term = 'term20'::report.enforcement_term OR cluster.overall_quality = 'mediocre'::report.foundation_quality OR cluster.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'drainage'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term25'::report.enforcement_term OR cluster.enforcement_term = 'term30'::report.enforcement_term OR cluster.enforcement_term = 'term40'::report.enforcement_term OR cluster.overall_quality = 'good'::report.foundation_quality OR cluster.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") cluster_dewatering_depth_risk(cluster_dewatering_depth_risk),
    LATERAL ( SELECT
                CASE
                    WHEN cluster.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term05'::report.enforcement_term OR cluster.enforcement_term = 'term5'::report.enforcement_term OR cluster.recovery_advised OR cluster.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term510'::report.enforcement_term OR cluster.enforcement_term = 'term10'::report.enforcement_term OR cluster.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term1020'::report.enforcement_term OR cluster.enforcement_term = 'term15'::report.enforcement_term OR cluster.enforcement_term = 'term20'::report.enforcement_term OR cluster.overall_quality = 'mediocre'::report.foundation_quality OR cluster.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN cluster.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (cluster.enforcement_term = 'term25'::report.enforcement_term OR cluster.enforcement_term = 'term30'::report.enforcement_term OR cluster.enforcement_term = 'term40'::report.enforcement_term OR cluster.overall_quality = 'good'::report.foundation_quality OR cluster.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") cluster_bio_infection_risk(cluster_bio_infection_risk),
    LATERAL ( SELECT
                CASE
                    WHEN cluster_recovery_sample.type IS NOT NULL THEN 'e'::data.foundation_risk_indication
                    WHEN cluster.enforcement_term = 'term05'::report.enforcement_term OR cluster.enforcement_term = 'term5'::report.enforcement_term OR cluster.enforcement_term = 'term510'::report.enforcement_term OR cluster.enforcement_term = 'term10'::report.enforcement_term OR cluster.enforcement_term = 'term15'::report.enforcement_term OR cluster.enforcement_term = 'term1020'::report.enforcement_term OR cluster.enforcement_term = 'term20'::report.enforcement_term OR cluster.recovery_advised OR cluster.overall_quality = 'bad'::report.foundation_quality OR cluster.overall_quality = 'mediocre_bad'::report.foundation_quality OR cluster.overall_quality = 'mediocre'::report.foundation_quality OR cluster.damage_cause IS NOT NULL THEN 'd'::data.foundation_risk_indication
                    WHEN cluster.enforcement_term = 'term25'::report.enforcement_term OR cluster.enforcement_term = 'term30'::report.enforcement_term OR cluster.enforcement_term = 'term40'::report.enforcement_term OR cluster.overall_quality = 'good'::report.foundation_quality OR cluster.overall_quality = 'mediocre_good'::report.foundation_quality OR cluster.overall_quality = 'tolerable'::report.foundation_quality THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") cluster_unclassified_risk(cluster_unclassified_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN (established.damage_cause = 'drystand'::report.foundation_damage_cause OR established.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR established.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (established.enforcement_term = 'term05'::report.enforcement_term OR established.enforcement_term = 'term5'::report.enforcement_term OR established.recovery_advised OR established.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN (established.damage_cause = 'drystand'::report.foundation_damage_cause OR established.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR established.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (established.enforcement_term = 'term510'::report.enforcement_term OR established.enforcement_term = 'term10'::report.enforcement_term OR established.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN (established.damage_cause = 'drystand'::report.foundation_damage_cause OR established.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR established.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (established.enforcement_term = 'term1020'::report.enforcement_term OR established.enforcement_term = 'term15'::report.enforcement_term OR established.enforcement_term = 'term20'::report.enforcement_term OR established.overall_quality = 'mediocre'::report.foundation_quality OR established.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN (established.damage_cause = 'drystand'::report.foundation_damage_cause OR established.damage_cause = 'fungus_infection'::report.foundation_damage_cause OR established.damage_cause = 'bio_fungus_infection'::report.foundation_damage_cause) AND (established.enforcement_term = 'term25'::report.enforcement_term OR established.enforcement_term = 'term30'::report.enforcement_term OR established.enforcement_term = 'term40'::report.enforcement_term OR established.overall_quality = 'good'::report.foundation_quality OR established.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") established_drystand_risk(established_drystand_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'drainage'::report.foundation_damage_cause AND (established.enforcement_term = 'term05'::report.enforcement_term OR established.enforcement_term = 'term5'::report.enforcement_term OR established.recovery_advised OR established.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'drainage'::report.foundation_damage_cause AND (established.enforcement_term = 'term510'::report.enforcement_term OR established.enforcement_term = 'term10'::report.enforcement_term OR established.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'drainage'::report.foundation_damage_cause AND (established.enforcement_term = 'term1020'::report.enforcement_term OR established.enforcement_term = 'term15'::report.enforcement_term OR established.enforcement_term = 'term20'::report.enforcement_term OR established.overall_quality = 'mediocre'::report.foundation_quality OR established.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'drainage'::report.foundation_damage_cause AND (established.enforcement_term = 'term25'::report.enforcement_term OR established.enforcement_term = 'term30'::report.enforcement_term OR established.enforcement_term = 'term40'::report.enforcement_term OR established.overall_quality = 'good'::report.foundation_quality OR established.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") established_dewatering_depth_risk(established_dewatering_depth_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (established.enforcement_term = 'term05'::report.enforcement_term OR established.enforcement_term = 'term5'::report.enforcement_term OR established.recovery_advised OR established.overall_quality = 'bad'::report.foundation_quality) THEN 'e'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (established.enforcement_term = 'term510'::report.enforcement_term OR established.enforcement_term = 'term10'::report.enforcement_term OR established.overall_quality = 'mediocre_bad'::report.foundation_quality) THEN 'd'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (established.enforcement_term = 'term1020'::report.enforcement_term OR established.enforcement_term = 'term15'::report.enforcement_term OR established.enforcement_term = 'term20'::report.enforcement_term OR established.overall_quality = 'mediocre'::report.foundation_quality OR established.overall_quality = 'tolerable'::report.foundation_quality) THEN 'c'::data.foundation_risk_indication
                    WHEN established.damage_cause = 'bio_infection'::report.foundation_damage_cause AND (established.enforcement_term = 'term25'::report.enforcement_term OR established.enforcement_term = 'term30'::report.enforcement_term OR established.enforcement_term = 'term40'::report.enforcement_term OR established.overall_quality = 'good'::report.foundation_quality OR established.overall_quality = 'mediocre_good'::report.foundation_quality) THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") established_bio_infection_risk(established_bio_infection_risk),
    LATERAL ( SELECT
                CASE
                    WHEN recovery.type IS NOT NULL THEN 'a'::data.foundation_risk_indication
                    WHEN established.enforcement_term = 'term05'::report.enforcement_term OR established.enforcement_term = 'term5'::report.enforcement_term OR established.enforcement_term = 'term510'::report.enforcement_term OR established.enforcement_term = 'term10'::report.enforcement_term OR established.enforcement_term = 'term15'::report.enforcement_term OR established.enforcement_term = 'term1020'::report.enforcement_term OR established.enforcement_term = 'term20'::report.enforcement_term OR established.recovery_advised OR established.overall_quality = 'bad'::report.foundation_quality OR established.overall_quality = 'mediocre_bad'::report.foundation_quality OR established.overall_quality = 'mediocre'::report.foundation_quality OR established.damage_cause IS NOT NULL THEN 'e'::data.foundation_risk_indication
                    WHEN established.enforcement_term = 'term25'::report.enforcement_term OR established.enforcement_term = 'term30'::report.enforcement_term OR established.enforcement_term = 'term40'::report.enforcement_term OR established.overall_quality = 'good'::report.foundation_quality OR established.overall_quality = 'mediocre_good'::report.foundation_quality OR established.overall_quality = 'tolerable'::report.foundation_quality THEN 'b'::data.foundation_risk_indication
                    ELSE NULL::data.foundation_risk_indication
                END AS "case") established_unclassified_risk(established_unclassified_risk)
  WHERE ba.building_type = 'house'::geocoder.building_type;

ALTER TABLE data.model_risk_dynamic_all
    OWNER TO fundermaps;

GRANT ALL ON TABLE data.model_risk_dynamic_all TO fundermaps;
GRANT SELECT ON TABLE data.model_risk_dynamic_all TO fundermaps_webapp;
GRANT SELECT ON TABLE data.model_risk_dynamic_all TO fundermaps_webservice;

