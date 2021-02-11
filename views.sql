/**
 * Clean up
 * We're going to recreate all views in the following...
 */
DROP VIEW IF EXISTS view_latency_inserts;
DROP VIEW IF EXISTS view_latency_updates;
DROP VIEW IF EXISTS view_graphalytics_inserts; /* dependency on view_insert_only */
DROP VIEW IF EXISTS view_inserts;
DROP VIEW IF EXISTS view_graphalytics_updates; /* dependency on view_aging */
DROP VIEW IF EXISTS view_updates_progress;
DROP VIEW IF EXISTS view_updates;
DROP VIEW IF EXISTS view_updates0;
DROP VIEW IF EXISTS view_graphalytics_load;
DROP VIEW IF EXISTS view_executions; /* Keep at the end due to dependencies */

/**
 * The first view to create!
 * view_executions reports all parameters of the experiment, the type, the start and end time,
 * the machines where the experiment was executed, and so on
 */
CREATE VIEW view_executions AS
SELECT
    e.id AS exec_id,
    CASE
        WHEN(is_insert_only.id IS NOT NULL) THEN 'insert_only'
        WHEN(is_aging.id IS NOT NULL) THEN CASE WHEN(aging_impl IS NULL) THEN 'aging1' ELSE 'aging2' END
        WHEN(e.load IS NOT NULL AND CAST(e.load AS INT)) THEN 'load'
        ELSE NULL
        END AS 'experiment',
    e.library,
    COALESCE(CAST(e.aging AS REAL), 0.0) AS 'aging',
    COALESCE(CAST(e.aging_cooloff AS INT), 0) AS 'aging_cooloff_secs',
    COALESCE(CAST(e.aging_release_memory AS INT), /* default = true */ 1) AS 'aging_release_memory',
    COALESCE(CAST(e.aging_step_size AS REAL), 1.0) AS 'aging_step',
    COALESCE(CAST(e.aging_timeout AS INT), 0) AS 'aging_timeout',
    COALESCE(CAST(e.batch AS INT), 0) AS 'batch_sz',
    CASE
        WHEN(e.compiler IS NULL) THEN NULL
        WHEN(e.compiler = 'clang') THEN ('Clang ' || e.compiler_major || '.' || e.compiler_minor || (CASE (e.compiler_patch) WHEN NULL THEN '' WHEN '0' THEN '' ELSE ('.' || e.compiler_patch) END) )
        WHEN(e.compiler = 'gcc') THEN ('GCC ' || e.compiler_major || '.' || e.compiler_minor || (CASE (e.compiler_patch) WHEN NULL THEN '' WHEN '0' THEN '' ELSE ('.' || e.compiler_patch) END) )
        WHEN(e.compiler = 'icc') THEN ('Intel ICC ' || e.compiler_major || '.' || e.compiler_minor || (CASE (e.compiler_patch) WHEN NULL THEN '' WHEN '0' THEN '' ELSE ('.' || e.compiler_patch) END) )
        ELSE e.compiler || ' ' || e.compiler_major || '.' || e.compiler_minor || (CASE (e.compiler_patch) WHEN NULL THEN '' WHEN '0' THEN '' ELSE ('.' || e.compiler_patch) END)
        END AS 'compiler',
    e.compiler AS 'compiler_family',
    CAST(e.compiler_major AS INTEGER) AS 'compiler_major',
    CAST(e.compiler_minor AS INTEGER) AS 'compiler_minor',
    CAST(e.compiler_patch AS INTEGER) AS 'compiler_patch_level',
    COALESCE((SELECT 1 FROM aging_intermediate_memory_usage_v2 mem WHERE mem.exec_id = e.id), /* false */ 0) AS memfp,
    COALESCE(CAST(e.aging_memfp_report AS INT), 0) AS memfp_report,
    COALESCE(CAST(e.aging_memfp_physical AS INT), /* by default, yes */ 1) AS memfp_rss,
    COALESCE(CAST(e.aging_memfp_threshold AS INT), 0) AS memfp_threshold_bytes,
    CAST(e.num_repetitions AS INT) AS 'num_repetitions',
    CAST(e.num_threads_read AS INT) AS 'num_threads_read',
    CAST(e.num_threads_write AS INT) AS 'num_threads_write',
    -- Also transform num_threads_omp = 0 => NULL for compatibility with the notebook already created with Stinger
    CASE WHEN(e.num_threads_omp IS NULL OR e.num_threads_omp = '0') THEN NULL ELSE CAST(e.num_threads_omp AS INT) END AS 'num_threads_omp',
    COALESCE(CAST(e.measure_latency AS INT), 0) AS 'has_latency',
    COALESCE(CAST(e.num_validation_errors AS INT), -1) AS 'num_validation_errors',
    COALESCE(CAST(e.build_frequency AS INT), 0) AS 'build_frequency_millisecs',
    COALESCE(CAST(e.timeout AS INT), 0) AS 'timeout',
    COALESCE(CAST(e.directed AS INT), /* assume directed */ 1) AS 'is_directed',
    e.graph AS 'client_graph',
    COALESCE(role, 'client-server') AS 'mode', /* obsolete property */
    e.client_host AS 'client_host',
    e.server_host AS 'server_host',
    COALESCE(CAST(e.server_port AS INT), -1) AS 'server_port',
    COALESCE(e.hostname, e.server_host) AS 'hostname',
    CASE WHEN(e.server_host IS NULL) THEN
             CASE WHEN(INSTR(e.hostname, "stones2") > 0) THEN 'stones2'
                  WHEN(INSTR(e.hostname, "rocks2") > 0) THEN 'rocks2'
                  WHEN(INSTR(e.hostname, "diamonds") > 0) THEN 'diamonds'
                  WHEN(INSTR(e.hostname, "bricks") > 0) THEN 'bricks'
                  ELSE 'unknown' END
         ELSE
             CASE WHEN(INSTR(e.server_host, "stones2") > 0) THEN 'stones2'
                  WHEN(INSTR(e.server_host, "rocks2") > 0) THEN 'rocks2'
                  WHEN(INSTR(e.server_host, "diamonds") > 0) THEN 'diamonds'
                  WHEN(INSTR(e.server_host, "bricks") > 0) THEN 'bricks'
                  ELSE 'unknown' END
        END AS 'cluster',
    e.git_commit AS 'git_commit',
    COALESCE(aging_impl, 'version_1') AS 'aging_impl',
    e.time_start,
    e.time_end
FROM executions e
         LEFT JOIN insert_only is_insert_only ON is_insert_only.exec_id = e.id
         LEFT JOIN aging is_aging ON is_aging.exec_id = e.id
/*
    17/Mar/2019 - Remove all the executions of GraphOne with S.F. 26 that were executed before this date. There was an
    issue with the vertex dictionary. It is statically allocated in the GraphOne library (not in the driver) and it did
    not have enough space to store all vertices, causing a memory overflow.
    Bug fixed in commit 0143b4ec on 17/Mar/2019, graphone library (not gfe driver), branch feature/gfe
 */
WHERE
    NOT (graph LIKE '%-26.properties' AND library LIKE 'g1-%' AND time_start < '2020-03-17')
;

/**
 * Retrieve the results from the experiment 'insert_only'
 * That is how long it took for a library to load the graph using `num_threads' and
 * only relying on calls to insert a vertex and an edge at the time.
 */
CREATE VIEW view_inserts AS
SELECT
    e.exec_id,
    e.cluster,
    e.hostname,
    e.mode,
    e.library, /* the implementation tested */
    e.client_graph, /* the graph used, full absolute path relative to the client_host */
    e.is_directed, /* is the graph directed ? */
    -- e.batch_sz, /* if the insertions were sent from the client to the server in batches, report the size of each batch in terms of number of edges */
    e.compiler_family,
    e.compiler,
    e.num_threads_write AS num_threads, /* num threads used in the client/server */
    e.num_threads_omp AS omp_threads, /* num threads configured to be used in OMP */
    e.num_validation_errors, /*  -1 => no validations performed; otherwise it's the number of edges missing from the original graph */
    e.has_latency, /* whether the experiment measured also the latency of the operations */
    io.scheduler, /* implementation detail, the internal algorithm used in the experiment to schedule the insertions among the used threads */
    CASE /* before 25/11/2019, the parameter --build_frequency was ignore and only one snapshot was created at the end of the experiment */
        WHEN(CAST(io.revision AS INTEGER) < 20191125) THEN 0
        ELSE e.build_frequency_millisecs
        END AS 'build_frequency_millisecs', /* how often manually create a new level/snapshot/delta, invoking the method #build() */
    io.num_build_invocations, /* the total number of levels/snapshots/deltas created, by manually invoking the method #build() */
    io.num_snapshots_created, /* total number of levels/snapshots/deltas created, either by explicitly invoking the method #build() or implicitly by the library */
    io.insertion_time AS insertion_time_usecs, /* total time to insert the elements, excl. building the final snapshot */
    100.0 * (io.insertion_time) / (io.insertion_time + io.build_time) AS insertion_time_perc,
    io.build_time AS build_time_usecs, /* total time to build the *last* snapshot in LLAMA */
    100.0 * (io.build_time) / (io.insertion_time + io.build_time) AS build_time_perc,
    io.insertion_time + io.build_time AS completion_time_usecs /* microsecs */
FROM view_executions e JOIN insert_only io ON(e.exec_id = io.exec_id)
WHERE e.mode = 'standalone'
;

/**
 * Retrieve the results from the experiment `graphalytics', after the graph
 * has been constructed with only inserts (no updates, no aging)
 */
CREATE VIEW view_graphalytics_inserts AS
SELECT
  i.library,
  i.cluster,
  i.client_graph,
  i.is_directed,
  i.compiler_family,
  i.compiler,
  i.build_frequency_millisecs, /* this needs to be accounted for llama */
  i.num_snapshots_created, /* as above */
  e.num_threads_read AS num_threads_read,
  e.num_threads_write AS num_threads_write,
  e.num_threads_omp AS omp_threads,
  s.type AS algorithm,
  s.mean AS mean_usecs,
  s.median AS median_usecs,
  s.min AS min_usecs,
  s.max AS max_usecs,
  s.p90 AS p90_usecs,
  s.p95 AS p95_usecs,
  s.p97 AS p97_usecs,
  s.p99 AS p99_usecs,
  s.num_trials,
  s.num_timeouts,
  s.num_trials = s.num_timeouts AS is_all_timeout
FROM view_inserts i
  JOIN statistics s ON( i.exec_id = s.exec_id )
  JOIN view_executions e ON ( i.exec_id = e.exec_id )
;

/**
 * Final results from the `aging' experiment. Report the amount to insert/delete (updates) the given graph
 * by performing `aging'x times updates
 */
CREATE VIEW view_updates AS
WITH tp3 AS (
    SELECT exec_id,  MAX(num_operations) / MAX(second) AS throughput
    FROM aging_intermediate_throughput3
    GROUP BY (exec_id)
),
    /*
    Get the final (last recorded) memory usage for the execution
    */
     mem3 AS (
         SELECT mem1.exec_id, (memfp_process - memfp_driver) AS memory_footprint
         FROM
             ( SELECT exec_id, MAX(tick) AS tick FROM aging_intermediate_memory_usage_v2 WHERE cooloff = 0 GROUP BY (exec_id) ) AS mem0
                 JOIN aging_intermediate_memory_usage_v2 mem1
         WHERE mem0.exec_id = mem1.exec_id AND mem0.tick = mem1.tick
     )
SELECT
    e.exec_id,
    e.cluster,
    e.library,
    e.aging,
    e.client_graph,
    e.is_directed,
    e.compiler_family,
    e.compiler,
    a.num_threads,
    e.num_threads_omp AS omp_threads,
    e.has_latency,
    CASE WHEN (a.has_terminated_for_timeout ) THEN 'timeout'
         WHEN (a.has_terminated_for_memfp) THEN 'memory_overflow'
         ELSE 'completed'
    END AS outcome,
    a.num_updates AS num_edge_updates,
    e.build_frequency_millisecs,
    a.num_build_invocations,
    a.num_snapshots_created,
    a.completion_time AS completion_time_usecs,
    CASE WHEN ( a.has_terminated_for_timeout OR a.has_terminated_for_memfp ) THEN
        tp3.throughput
    ELSE
        a.num_updates / (a.completion_time /* microsecs, convert in secs */ / 1000 / 1000)
    END AS throughput,
    /*
        If driver_release_memory = 1 (true), then the driver released the log buffers while running the experiment.
        The execution times may be different when this is enabled or disabled, because of the additional memory
        usage in the process.
        The readings on the memory_footprint will be definitely different however.
     */
    e.aging_release_memory AS driver_release_memory,
    e.memfp AS memfp, -- whether the memory footprint was measured
    e.memfp_rss AS memfp_rss,
    mem3.memory_footprint AS memfp_bytes
FROM view_executions e
         JOIN aging a ON (e.exec_id = a.exec_id)
         JOIN tp3 ON (e.exec_id = tp3.exec_id)
         LEFT JOIN mem3 ON (e.exec_id = mem3.exec_id)
WHERE e.mode = 'standalone'
  AND aging_impl IN ('version_2', 'version_3') AND batch_sz = 0
  AND ((NOT a.has_terminated_for_timeout AND NOT a.has_terminated_for_memfp) OR (tp3.throughput IS NOT NULL))
;

/**
 * Show how long it took to perform 1x, 2x, 3x, ... updates w.r.t. the number of edges in the final graph
 */
CREATE VIEW view_updates_progress AS
WITH
    /*
       For some reason, sometimes it does not save in aging_intermediate_throughput the progress for the last chunk (typically 10),
       but we can infer it from the total execution time in the table `aging' (or view_updates). It is not the same as if it was
       reported in the table aging_intermediate_throughput though, especially in delta stores, because in this other case it also
       includes the time to build a new snapshot and terminate the worker threads in the experiment.
     */
    complete_intermediates AS (
        SELECT u.exec_id, ROUND(u.aging / e.aging_step, 2) AS aging_coeff, u.completion_time_usecs AS completion_time
        FROM view_updates u JOIN view_executions e ON (u.exec_id = e.exec_id)
        WHERE NOT EXISTS ( SELECT 1 FROM aging_intermediate_throughput i WHERE u.exec_id = i.exec_id AND ROUND(u.aging / e.aging_step, 2) = i.aging_coeff)
        UNION ALL
        SELECT exec_id, aging_coeff, completion_time
        FROM aging_intermediate_throughput
    ),
    deltas(exec_id, aging_coeff, completion_time, delta) AS (
        SELECT t.exec_id, t.aging_coeff, t.completion_time, t.completion_time AS delta
        FROM complete_intermediates t WHERE t.aging_coeff = 1
        UNION ALL
        SELECT t.exec_id, t.aging_coeff, t.completion_time, t.completion_time - d.completion_time AS delta
        FROM complete_intermediates t, deltas d
        WHERE t.exec_id = d.exec_id AND t.aging_coeff = d.aging_coeff + 1
    )
SELECT
    e.exec_id,
    e.library,
    e.cluster,
    e.aging,
    e.client_graph,
    e.is_directed,
    e.num_threads_write AS num_threads,
    e.num_threads_omp AS omp_threads,
    e.has_latency,
    ROUND(d.aging_coeff * e.aging_step, 2) AS progress, -- normalise the progress
    d.completion_time AS completion_time_usecs,
    d.delta AS delta_usecs
FROM view_executions e, deltas d
WHERE e.exec_id = d.exec_id AND e.aging_impl IN ('version_2', 'version_3') AND e.mode = 'standalone'
;

/**
 * Report the throughput as edges/sec recorded for the aging (updates) experiment.
 */
CREATE VIEW view_updates_throughput AS
SELECT
    e.exec_id,
    e.library,
    e.cluster,
    e.aging,
    e.client_graph,
    e.is_directed,
    e.num_threads_write AS num_threads,
    e.num_threads_omp AS omp_threads,
    e.has_latency,
    a.second,
    a.num_operations,
    COALESCE(a.num_operations - (LAG(a.num_operations) OVER (PARTITION BY a.exec_id ORDER BY a.second)), a.num_operations) as throughput
FROM aging_intermediate_throughput3 a JOIN view_executions e on a.exec_id = e.exec_id;

/**
 * Report the memory footprint, in bytes, during the updates, every 10 secs.
 */
CREATE VIEW view_updates_memory_footprint AS
SELECT
    e.exec_id,
    e.library,
    e.cluster,
    e.aging,
    e.client_graph,
    e.is_directed,
    e.aging_release_memory AS driver_release_memory,
    e.num_threads_write AS num_threads,
    e.num_threads_omp AS omp_threads,
    e.has_latency,
    mem.tick AS second,
    (CAST( ait3.num_operations AS REAL ) / (SELECT u.num_updates FROM aging u WHERE u.exec_id = ait3.exec_id)) AS progress, /* in [0, 1] */
    (mem.memfp_process - mem.memfp_driver) AS memory_usage_bytes
FROM aging_intermediate_memory_usage_v2 mem
         JOIN view_executions e on mem.exec_id = e.exec_id
         JOIN aging_intermediate_throughput3 ait3 ON (ait3.exec_id = mem.exec_id AND ait3.second = mem.tick)
WHERE mem.cooloff = 0
;

/**
 * Retrieve the results from the experiment `graphalytics', after the graph
 * has been constructed with a mix of insertions/updates/deletions
 */
CREATE VIEW view_graphalytics_updates AS
SELECT
  u.exec_id,
  u.library,
  u.cluster,
  u.aging,
  u.client_graph,
  u.is_directed,
  u.compiler_family,
  u.compiler,
  e.num_threads_read AS num_threads,
  e.num_threads_omp AS omp_threads,
  s.type AS algorithm,
  s.mean AS mean_usecs,
  s.median AS median_usecs,
  s.min AS min_usecs,
  s.max AS max_usecs,
  s.p90 AS p90_usecs,
  s.p95 AS p95_usecs,
  s.p97 AS p97_usecs,
  s.p99 AS p99_usecs,
  s.num_trials,
  s.num_timeouts,
    s.num_trials = s.num_timeouts AS is_all_timeout
FROM
  view_updates u
  JOIN statistics s ON( u.exec_id = s.exec_id )
  JOIN view_executions e ON ( s.exec_id = e.exec_id )
;

CREATE VIEW view_latency_inserts AS
SELECT
  i.cluster,
  i.library,
  i.client_graph,
  i.is_directed,
  i.num_threads,
  i.omp_threads,
  l.num_operations,
  l.mean AS mean_nanosecs,
  l.median AS median_nanosecs,
  l.min AS min_nanosecs,
  l.max AS max_nanosecs,
  l.p90 AS p90_nanosecs,
  l.p95 AS p95_nanosecs,
  l.p97 AS p97_nanosecs,
  l.p99 AS p99_nanosecs
FROM view_inserts i JOIN latencies l ON (i.exec_id = l.exec_id)
;

CREATE VIEW view_latency_updates AS
SELECT
  u.cluster,
  u.library,
  u.aging,
  u.client_graph,
  u.is_directed,
  u.num_threads,
  u.omp_threads,
  u.num_build_invocations,
  u.num_snapshots_created,
  lIns.num_operations AS num_insertions,
  lIns.mean AS inserts_mean_nanosecs,
  lIns.median AS inserts_median_nanosecs,
  lIns.min AS inserts_min_nanosecs,
  lIns.max AS inserts_max_nanosecs,
  lIns.p90 AS inserts_p90_nanosecs,
  lIns.p95 AS inserts_p95_nanosecs,
  lIns.p97 AS inserts_p97_nanosecs,
  lIns.p99 AS inserts_p99_nanosecs,
  lDel.num_operations AS num_deletions,
  lDel.mean AS deletes_mean_nanosecs,
  lDel.median AS deletes_median_nanosecs,
  lDel.min AS deletes_min_nanosecs,
  lDel.max AS deletes_max_nanosecs,
  lDel.p90 AS deletes_p90_nanosecs,
  lDel.p95 AS deletes_p95_nanosecs,
  lDel.p97 AS deletes_p97_nanosecs,
  lDel.p99 AS deletes_p99_nanosecs,
  lUpd.num_operations AS num_updates,
  lUpd.mean AS updates_mean_nanosecs,
  lUpd.median AS updates_median_nanosecs,
  lUpd.min AS updates_min_nanosecs,
  lUpd.max AS updates_max_nanosecs,
  lUpd.p90 AS updates_p90_nanosecs,
  lUpd.p95 AS updates_p95_nanosecs,
  lUpd.p97 AS updates_p97_nanosecs,
  lUpd.p99 AS updates_p99_nanosecs
FROM view_updates u
     JOIN latencies lIns ON (u.exec_id = lIns.exec_id AND lIns.type = 'inserts')
     JOIN latencies lDel ON (u.exec_id = lDel.exec_id AND lDel.type = 'deletes')
     JOIN latencies lUpd ON (u.exec_id = lUpd.exec_id AND lUpd.type = 'updates')
;

/**
 * Profiling of graphalytics (brittle implementation due to the usage of Percent_Rank)
 */
CREATE VIEW view_graphalytics_profiler_inserts AS
WITH
    total AS (
        SELECT
            e.library,
            e.cluster,
            e.client_graph,
            e.is_directed,
            e.build_frequency_millisecs, /* this needs to be accounted for llama */
            vi.num_snapshots_created,
            e.num_threads_read                                             AS num_threads_read,
            e.num_threads_write                                            AS num_threads_write,
            e.num_threads_omp                                              AS omp_threads,
            gp.*,
            gp.cache_l1_misses + gp.cache_llc_misses + gp.cache_tlb_misses AS total_misses
        FROM graphalytics_profiler gp
        JOIN view_executions e ON e.exec_id = gp.exec_id
        JOIN view_inserts vi ON vi.exec_id = gp.exec_id
    ),
    ranks AS (
        SELECT *, PERCENT_RANK() OVER(PARTITION BY library, cluster, client_graph, algorithm ORDER BY total_misses) AS rank
        FROM total
    )
SELECT library, cluster, client_graph, algorithm, cache_l1_misses, cache_llc_misses, cache_tlb_misses
FROM ranks
WHERE rank = 0.5
;

/**
 * Retrieve the results from the experiment `graphalytics', after the graph
 * has been directly loaded from the file
 */
CREATE VIEW view_graphalytics_load AS
SELECT
    e.library,
    e.cluster,
    e.client_graph,
    e.is_directed,
    e.compiler_family,
    e.compiler,
    e.num_threads_read AS num_threads_read,
    e.num_threads_omp AS omp_threads,
    s.type AS algorithm,
    s.mean AS mean_usecs,
    s.median AS median_usecs,
    s.min AS min_usecs,
    s.max AS max_usecs,
    s.p90 AS p90_usecs,
    s.p95 AS p95_usecs,
    s.p97 AS p97_usecs,
    s.p99 AS p99_usecs,
    s.num_trials,
    s.num_timeouts,
    s.num_trials = s.num_timeouts AS is_all_timeout
FROM statistics s
         JOIN view_executions e ON ( s.exec_id = e.exec_id )
WHERE e.experiment = 'load'
;