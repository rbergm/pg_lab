
#ifndef PATHGEN_H
#define PATHGEN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes/pathnodes.h"

extern void force_seqscan_paths(PlannerInfo *root, RelOptInfo *rel);

extern void force_idxscan_paths(PlannerInfo *root, RelOptInfo *rel);

extern void force_nestloop_paths(PlannerInfo *root, RelOptInfo *joinrel,
                                 RelOptInfo *outerrel, RelOptInfo *innerrel,
                                 JoinType jointype, JoinPathExtraData *extra);

extern void force_mergejoin_paths(PlannerInfo *root, RelOptInfo *joinrel,
                                  RelOptInfo *outerrel, RelOptInfo *innerrel,
                                  JoinType jointype, JoinPathExtraData *extra);

extern void force_hashjoin_paths(PlannerInfo *root, RelOptInfo *joinrel,
                                 RelOptInfo *outerrel, RelOptInfo *innerrel,
                                 JoinType jointype, JoinPathExtraData *extra);

#ifdef __cplusplus
}
#endif

#endif /* PATHGEN_H */
