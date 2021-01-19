CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.find_points_around_from_geojson`(geojson STRING, startx FLOAT64, starty FLOAT64, max_cost FLOAT64) 
RETURNS STRING LANGUAGE js
OPTIONS (
  library=["$BUCKET_FILE_PATH"]
)
AS """
  const start = {type: "Feature", geometry: { coordinates: [startx, starty], type: "Point" }};
  const pathFinder = new geojsonPathFinder(JSON.parse(geojson));

  const nodes = pathFinder.findPointsAround(start, max_cost);
  try {
    return JSON.stringify({
      type: "MultiPoint",
      coordinates: nodes
    });
  }
  catch (e) {
    return(null);
  }
""";

-- get the concave hull
CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isodistance_concave_hull_from_geojson`(geojson STRING, startx FLOAT64, starty FLOAT64, max_cost FLOAT64) 
RETURNS STRING LANGUAGE js
OPTIONS (
  library=["$BUCKET_FILE_PATH"]
)
AS """
  const start = {type: "Feature", geometry: { coordinates: [startx, starty], type: "Point" }};
  const pathFinder = new geojsonPathFinder(JSON.parse(geojson));

  const hull = pathFinder.getIsoDistanceConcaveHull(start, max_cost);
  
  try {
    return JSON.stringify(hull.geometry);
  } catch (e) {
    return(null);
  }
""";

-- get the convex hull
-- CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isodistance_convex_hull_from_geojson`(geojson STRING, startx FLOAT64, starty FLOAT64, max_cost FLOAT64) 
-- RETURNS STRING LANGUAGE js
-- OPTIONS (
--   library=["$BUCKET_FILE_PATH"]
-- )
-- AS """
--   const start = {type: "Feature", geometry: { coordinates: [startx, starty], type: "Point" }};
--   const pathFinder = new geojsonPathFinder(JSON.parse(geojson));

--   const hull = pathFinder.getIsoDistanceConvexHull(start, max_cost);
  
--   try {
--     return JSON.stringify(hull.geometry);
--   } catch (e) {
--     return(null);
--   }
-- """;

-- get isochrone the concave hull
CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isochrone_concave_hull_from_geojson`(geojson STRING, startx FLOAT64, starty FLOAT64, max_cost FLOAT64) 
RETURNS STRING LANGUAGE js
OPTIONS (
  library=["$BUCKET_FILE_PATH"]
)
AS """
  const highwaySpeeds = {
    motorway: 110,
    trunk: 90,
    primary: 80,
    secondary: 70,
    tertiary: 50,
    unclassified: 50,
    road: 50,
    residential: 30,
    service: 30,
    living_street: 20
  };

  const unknowns = {};

  function weightFn(a, b, props) {
      let d = distance(point(a), point(b)) * 1000,
          factor = 0.9,
          type = props.highway,
          forwardSpeed,
          backwardSpeed;

      if (props.maxspeed) {
          forwardSpeed = backwardSpeed = Number(props.maxspeed);
      } else {
          let linkIndex = type.indexOf('_link');

          if (linkIndex >= 0) {
              type = type.substring(0, linkIndex);
              factor *= 0.7;
          }
      
          forwardSpeed = backwardSpeed = highwaySpeeds[type] * factor;
      
          if (!forwardSpeed) {
              unknowns[type] = true;
          }
      }
      
      if (props.oneway && props.oneway !== 'no' || props.junction && props.junction === 'roundabout') {
          backwardSpeed = null;
      }

      return {
          forward: forwardSpeed && (d / (forwardSpeed / 3.6)),
          backward: backwardSpeed && (d / (backwardSpeed / 3.6)),
      };
  }

  const start = {type: "Feature", geometry: { coordinates: [startx, starty], type: "Point" }};
  const pathFinder = new geojsonPathFinder(JSON.parse(geojson), { weightFn: weightFn });

  const hull = pathFinder.getIsoDistanceConcaveHull(start, max_cost);
  
  try {
    return JSON.stringify(hull.geometry);
  } catch (e) {
    return(null);
  }
""";

-- get isochrone the convex hull
-- CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isochrone_convex_hull_from_geojson`(geojson STRING, startx FLOAT64, starty FLOAT64, max_cost FLOAT64) 
-- RETURNS STRING LANGUAGE js
-- OPTIONS (
--   library=["$BUCKET_FILE_PATH"]
-- )
-- AS """
--   const start = {type: "Feature", geometry: { coordinates: [startx, starty], type: "Point" }};
--   const pathFinder = new geojsonPathFinder(JSON.parse(geojson));

--   const hull = pathFinder.getIsoDistanceConvexHull(start, max_cost);
  
--   try {
--     return JSON.stringify(hull.geometry);
--   } catch (e) {
--     return(null);
--   }
-- """;

-- helper to find the nearest point if the input point is not in the dataset
CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.find_nearest_point`(mypoint GEOGRAPHY, mypoints array<GEOGRAPHY>) AS ((
  WITH EXTRACTED_POINTS AS (
    SELECT SAFE.ST_GEOGFROMTEXT(CONCAT('POINT(', point, ')')) mypoints
    FROM unnest(mypoints) geo_object,
      UNNEST(REGEXP_EXTRACT_ALL(ST_ASTEXT(geo_object), r'[^,\(\)]+')) point WITH OFFSET pos
    WHERE pos BETWEEN 1 AND ST_NUMPOINTS(geo_object)
  )
  SELECT ARRAY_AGG(a.mypoints ORDER BY ST_Distance(a.mypoints, mypoint) LIMIT 1)[ORDINAL(1)] as neighbor_id
  FROM EXTRACTED_POINTS a
));

-- wrapper for GEOGRAPHY to GEOJSON
CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.find_points_around`(lines array<GEOGRAPHY>, start GEOGRAPHY, max_cost FLOAT64) AS ((
  WITH SOME_NETWORK AS (
    SELECT concat('{"type": "FeatureCollection", "features": [{"type": "Feature","geometry":', string_agg(ST_ASGEOJSON(line), '},{"type":"Feature","geometry":'), "}]}") geojson,
    `$PROJECT_ID.$DATASET.find_nearest_point`(start, array_agg(line)) start_nearest,
    FROM unnest(lines) line
  ),
  OUTPUT AS (
    SELECT `$PROJECT_ID.$DATASET.find_points_around_from_geojson`(geojson, ST_X(start_nearest), ST_Y(start_nearest), max_cost) myresult
    FROM SOME_NETWORK
  )

  SELECT * FROM OUTPUT
));

-- wrapper for GEOGRAPHY to GEOJSON
CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isodistance_concave_hull`(lines array<GEOGRAPHY>, start GEOGRAPHY, max_cost FLOAT64) AS ((
  WITH SOME_NETWORK AS (
    SELECT concat('{"type": "FeatureCollection", "features": [{"type": "Feature","geometry":', string_agg(ST_ASGEOJSON(line), '},{"type":"Feature","geometry":'), "}]}") geojson,
    `$PROJECT_ID.$DATASET.find_nearest_point`(start, array_agg(line)) start_nearest,
    FROM unnest(lines) line
  ),
  OUTPUT AS (
    SELECT `$PROJECT_ID.$DATASET.get_isodistance_concave_hull_from_geojson`(geojson, ST_X(start_nearest), ST_Y(start_nearest), max_cost) myresult
    FROM SOME_NETWORK
  )

  SELECT * FROM OUTPUT
));

-- wrapper for GEOGRAPHY to GEOJSON
-- CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isodistance_convex_hull`(lines array<GEOGRAPHY>, start GEOGRAPHY, max_cost FLOAT64) AS ((
--   WITH SOME_NETWORK AS (
--     SELECT concat('{"type": "FeatureCollection", "features": [{"type": "Feature","geometry":', string_agg(ST_ASGEOJSON(line), '},{"type":"Feature","geometry":'), "}]}") geojson,
--     `$PROJECT_ID.$DATASET.find_nearest_point`(start, array_agg(line)) start_nearest,
--     FROM unnest(lines) line
--   ),
--   OUTPUT AS (
--     SELECT `$PROJECT_ID.$DATASET.get_isodistance_convex_hull_from_geojson`(geojson, ST_X(start_nearest), ST_Y(start_nearest), max_cost) myresult
--     FROM SOME_NETWORK
--   )

--   SELECT * FROM OUTPUT
-- ));

CREATE OR REPLACE FUNCTION `$PROJECT_ID.$DATASET.get_isodistance_convex_hull`(lines array<GEOGRAPHY>, start GEOGRAPHY, max_cost FLOAT64) AS ((
  SELECT ST_CONVEXHULL(ST_GEOGFROMGEOJSON(`$PROJECT_ID.$DATASET.find_points_around`(lines, start, max_cost)))
));

