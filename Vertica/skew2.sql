select schema_name,
        projection_name,
        big_node || '-' || small_node node_pair,
        size_pp(big_bytes - small_bytes) diff
from (
                SELECT distinct schema_name,
                        projection_name,
                        First_value(node_name) OVER (
                                w rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) big_node,
                        First_value(ros) OVER (
                                w rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) big_bytes,
                        last_value(node_name) OVER (
                                w rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) small_node,
                        last_value(ros) OVER (
                                w rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) small_bytes
                FROM (
                                SELECT schema_name,
                                        projection_name,
                                        node_name,
                                        Sum(used_bytes) ros
                                FROM storage_containers
                                GROUP BY 1,
                                        2,
                                        3
                        ) foo WINDOW w as (
                                partition BY schema_name,
                                projection_name
                                ORDER BY ros DESC
                        )
        ) foo
order by big_bytes - small_bytes desc;