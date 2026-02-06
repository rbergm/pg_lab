
#include <math.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/amapi.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

double scan_cost = 1.0;
double proc_cost = 1.2;
double ind_cost = 2.0;
Cost disable_cost = 1.0e11;

extern bool enable_seqscan;
extern bool enable_indexscan;
extern bool enable_indexonlyscan;
extern bool enable_bitmapscan;
extern bool enable_sort;
extern bool enable_incremental_sort;
extern bool enable_nestloop;
extern bool enable_material;
extern bool enable_memoize;
extern bool enable_mergejoin;
extern bool enable_hashjoin;

extern cost_seqscan_hook_type cost_seqscan_hook;
static cost_seqscan_hook_type prev_cost_seqscan_hook = NULL;

extern cost_index_hook_type cost_index_hook;
static cost_index_hook_type prev_cost_index_hook = NULL;

extern cost_bitmap_heap_scan_hook_type cost_bitmap_heap_scan_hook;
static cost_bitmap_heap_scan_hook_type prev_cost_bitmap_heap_scan_hook = NULL;

extern initial_cost_nestloop_hook_type initial_cost_nestloop_hook;
static initial_cost_nestloop_hook_type prev_initial_cost_nestloop_hook = NULL;
extern final_cost_nestloop_hook_type final_cost_nestloop_hook;
static final_cost_nestloop_hook_type prev_final_cost_nestloop_hook = NULL;

extern initial_cost_hashjoin_hook_type initial_cost_hashjoin_hook;
static initial_cost_hashjoin_hook_type prev_initial_cost_hashjoin_hook = NULL;
extern final_cost_hashjoin_hook_type final_cost_hashjoin_hook;
static final_cost_hashjoin_hook_type prev_final_cost_hashjoin_hook = NULL;

extern initial_cost_mergejoin_hook_type initial_cost_mergejoin_hook;
static initial_cost_mergejoin_hook_type prev_initial_cost_mergejoin_hook = NULL;
extern final_cost_mergejoin_hook_type final_cost_mergejoin_hook;
static final_cost_mergejoin_hook_type prev_final_cost_mergejoin_hook = NULL;

extern cost_sort_hook_type cost_sort_hook;
static cost_sort_hook_type prev_cost_sort_hook = NULL;
extern cost_incremental_sort_hook_type cost_incremental_sort_hook;
static cost_incremental_sort_hook_type prev_cost_incremental_sort_hook = NULL;

extern cost_material_hook_type cost_material_hook;
static cost_material_hook_type prev_cost_material_hook = NULL;

extern cost_memoize_rescan_hook_type cost_memoize_rescan_hook;
static cost_memoize_rescan_hook_type prev_cost_memoize_rescan_hook = NULL;

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_fini(void);

static bool cm_enabled = false;

void SetCoutStarCostModel(void);
void ResetCostModel(void);
void toggle_cost_model(bool newval, void* extra);

static void cout_cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info);
static void cout_cost_idxscan(IndexPath *path, PlannerInfo *root, double loop_count, bool partial_path);
static void cout_cost_bitmap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info,
                                  Path *bitmapqual, double loop_count);
static void cout_initial_cost_nlj(PlannerInfo *root, JoinCostWorkspace *workspace, JoinType jointype,
                                  Path *outer_path, Path *inner_path, JoinPathExtraData *extra);
static void cout_final_cost_nlj(PlannerInfo *root, NestPath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra);
static void cout_initial_cost_hashjoin(PlannerInfo *root, JoinCostWorkspace *workspace, JoinType jointype,
                                       List *hashclauses, Path *outer_path, Path *inner_path, JoinPathExtraData *extra,
                                       bool parallel_hash);
static void cout_final_cost_hashjoin(PlannerInfo *root, HashPath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra);


#if PG_VERSION_NUM >= 180000
static void cout_initial_cost_mergejoin(PlannerInfo *root,
                                        JoinCostWorkspace *workspace,
                                        JoinType jointype,
                                        List *mergeclauses,
                                        Path *outer_path, Path *inner_path,
                                        List *outersortkeys, List *innersortkeys,
                                        int outer_presorted_keys,
                                        JoinPathExtraData *extra);
#else
static void cout_initial_cost_mergejoin(PlannerInfo *root,
                                        JoinCostWorkspace *workspace,
                                        JoinType jointype,
                                        List *mergeclauses,
                                        Path *outer_path, Path *inner_path,
                                        List *outersortkeys, List *innersortkeys,
                                        JoinPathExtraData *extra);
#endif

static void cout_final_cost_mergejoin(PlannerInfo *root, MergePath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra);

#if PG_VERSION_NUM >= 180000
static void cout_cost_sort(Path *path, PlannerInfo *root,
                           List *pathkeys, int input_disabled_nodes,
                           Cost input_cost, double tuples, int width,
                           Cost comparison_cost, int sort_mem,
                           double limit_tuples);
#else
static void cout_cost_sort(Path *path, PlannerInfo *root,
                           List *pathkeys, Cost input_cost, double tuples, int width,
                           Cost comparison_cost, int sort_mem,
                           double limit_tuples);
#endif

#if PG_VERSION_NUM >= 180000
static void cout_cost_incremental_sort(Path *path, PlannerInfo *root, List *pathkeys, int presorted_keys,
                                       int input_disabled_nodes,
                                       Cost input_startup_cost, Cost input_total_cost, double input_tuples, int width,
                                       Cost comparison_cost, int sort_mem, double limit_tuples);
#else
static void cout_cost_incremental_sort(Path *path, PlannerInfo *root, List *pathkeys, int presorted_keys,
                                       Cost input_startup_cost, Cost input_total_cost, double input_tuples, int width,
                                       Cost comparison_cost, int sort_mem, double limit_tuples);
#endif

#if PG_VERSION_NUM >= 180000
static void cout_cost_materialize(Path *path,
                                  int input_disabled_nodes,
                                  Cost input_startup_cost, Cost input_total_cost,
                                  double tuples, int width);
#else
static void cout_cost_materialize(Path *path,
                                  Cost input_startup_cost, Cost input_total_cost,
                                  double tuples, int width);
#endif


static void cout_cost_memoize(PlannerInfo *root, MemoizePath *mpath, Cost *rescan_startup_cost, Cost *rescan_total_cost);


static void
cout_cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info)
{
    if (prev_cost_seqscan_hook)
        (*prev_cost_seqscan_hook)(path, root, baserel, param_info);
    else
        standard_cost_seqscan(path, root, baserel, param_info);

    path->startup_cost = 0;
    path->total_cost = scan_cost * baserel->tuples;

    if (!enable_seqscan)
    {
        path->startup_cost += disable_cost;
        path->total_cost += disable_cost;
    }
}

void
cout_cost_idxscan(IndexPath *path, PlannerInfo *root, double loop_count, bool partial_path)
{
    IndexOptInfo *index = path->indexinfo;
    RelOptInfo *baserel = index->rel;
    bool indexonly = (path->path.pathtype == T_IndexOnlyScan);
    Cost total_cost;

    if (prev_cost_index_hook)
        (*prev_cost_index_hook)(path, root, loop_count, partial_path);
    else
        standard_cost_index(path, root, loop_count, partial_path);

    if (indexonly)
    {
        total_cost = ind_cost * log10(baserel->tuples);
    }
    else
    {
        Cost startup_buffer, total_buffer;
        double corr_buffer, pages_buffer;
        Selectivity indsel;
        amcostestimate_function amcostestimate;

        amcostestimate = index->amcostestimate;
        amcostestimate(root, path, loop_count, &startup_buffer, &total_buffer, &indsel, &corr_buffer, &pages_buffer);

        total_cost = ind_cost * ((indsel * baserel->tuples) + log10(baserel->tuples));
    }

    path->path.startup_cost = 0;
    path->path.total_cost = total_cost;

    if ((!indexonly && !enable_indexscan) || (indexonly && !enable_indexonlyscan))
    {
        path->path.startup_cost += disable_cost;
        path->path.total_cost += disable_cost;
    }
}

typedef struct BitmapWalker
{
    int num_indexes;
    Selectivity current_sel;
} BitmapWalker;

static BitmapWalker
analyze_bitmap_scans(PlannerInfo *root, Path *path, double loop_count)
{
    ListCell *lc;
    Path *subpath;
    BitmapWalker subwalker;
    BitmapWalker result;

    if (IsA(path, IndexPath))
    {
        IndexPath *ipath = (IndexPath*) path;
        IndexOptInfo *index = ipath->indexinfo;
        Cost startup_buffer, total_buffer;
        double corr_buffer, pages_buffer;
        Selectivity indsel;
        amcostestimate_function amcostestimate;

        amcostestimate = index->amcostestimate;
        amcostestimate(root, ipath, loop_count, &startup_buffer, &total_buffer, &indsel, &corr_buffer, &pages_buffer);

        result.num_indexes = 1;
        result.current_sel = indsel;

    }
    else if (IsA(path, BitmapAndPath))
    {
        BitmapAndPath *and_path = (BitmapAndPath*) path;
        foreach (lc, and_path->bitmapquals)
        {
            subpath = (Path*) lfirst(lc);
            subwalker = analyze_bitmap_scans(root, subpath, loop_count);
            result.num_indexes += subwalker.num_indexes;
            result.current_sel *= subwalker.current_sel;
        }
    } else if (IsA(path, BitmapOrPath))
    {
        BitmapOrPath *or_path = (BitmapOrPath*) path;
        foreach (lc, or_path->bitmapquals)
        {
            subpath = (Path*) lfirst(lc);
            subwalker = analyze_bitmap_scans(root, subpath, loop_count);
            result.num_indexes += subwalker.num_indexes;
            result.current_sel += subwalker.current_sel;
        }
    } else
    {
        Assert(false);
    }

    return result;
}

void
cout_cost_bitmap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel, ParamPathInfo *param_info,
                      Path *bitmapqual, double loop_count)
{
    BitmapHeapPath *bm_path;
    BitmapWalker bitmap_info;

    Assert(IsA(path, BitmapHeapPath));
    bm_path = (BitmapHeapPath*) path;

    if (prev_cost_bitmap_heap_scan_hook)
        (*prev_cost_bitmap_heap_scan_hook)(path, root, baserel, param_info, bitmapqual, loop_count);
    else
        standard_cost_bitmap_heap_scan(path, root, baserel, param_info, bitmapqual, loop_count);

    bitmap_info = analyze_bitmap_scans(root, bitmapqual, loop_count);

    bm_path->path.startup_cost = 0;
    bm_path->path.total_cost = ind_cost * proc_cost * bitmap_info.num_indexes * log10(baserel->tuples);
    bm_path->path.total_cost += scan_cost * bitmap_info.current_sel * baserel->tuples;

    if (!enable_bitmapscan)
    {
        bm_path->path.startup_cost += disable_cost;
        bm_path->path.total_cost += disable_cost;
    }
}

void
cout_initial_cost_nlj(PlannerInfo *root, JoinCostWorkspace *workspace, JoinType jointype,
                      Path *outer_path, Path *inner_path,
                      JoinPathExtraData *extra)
{
    Cardinality tuples_to_proc;
    Cost child_cost;

    if (prev_initial_cost_nestloop_hook)
        (*prev_initial_cost_nestloop_hook)(root, workspace, jointype, outer_path, inner_path, extra);
    else
        standard_initial_cost_nestloop(root, workspace, jointype, outer_path, inner_path, extra);

    tuples_to_proc = outer_path->parent->rows * inner_path->parent->rows;
    child_cost = outer_path->total_cost + inner_path->total_cost;

    workspace->startup_cost = outer_path->startup_cost + inner_path->startup_cost;
    workspace->total_cost = workspace->startup_cost + tuples_to_proc + child_cost;

    if (!enable_nestloop)
    {
        workspace->startup_cost += disable_cost;
        workspace->total_cost += disable_cost;
    }
}


void
cout_final_cost_nlj(PlannerInfo *root, NestPath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra)
{
    JoinPath *jpath;
    Cardinality tuples_to_proc;
    Cost child_cost;

    if (prev_final_cost_nestloop_hook)
        (*prev_final_cost_nestloop_hook)(root, path, workspace, extra);
    else
        standard_final_cost_nestloop(root, path, workspace, extra);

    jpath = &(path->jpath);
    tuples_to_proc = jpath->outerjoinpath->parent->rows * jpath->innerjoinpath->parent->rows;
    child_cost = jpath->outerjoinpath->total_cost + jpath->innerjoinpath->total_cost;

    jpath->path.startup_cost = jpath->outerjoinpath->startup_cost + jpath->innerjoinpath->startup_cost;
    jpath->path.total_cost = jpath->path.startup_cost + tuples_to_proc + child_cost;

    if (!enable_nestloop)
    {
        jpath->path.startup_cost += disable_cost;
        jpath->path.total_cost += disable_cost;
    }
}


void
cout_initial_cost_hashjoin(PlannerInfo *root, JoinCostWorkspace *workspace, JoinType jointype,
						   List *hashclauses, Path *outer_path, Path *inner_path,
						   JoinPathExtraData *extra, bool parallel_hash)
{
    Cost hash_cost, probe_cost;

    if (prev_initial_cost_hashjoin_hook)
        (*prev_initial_cost_hashjoin_hook)(root, workspace, jointype,
                                           hashclauses, outer_path, inner_path,
                                           extra, parallel_hash);
    else
        standard_initial_cost_hashjoin(root, workspace, jointype, hashclauses, outer_path, inner_path, extra, parallel_hash);

    hash_cost = proc_cost * inner_path->parent->rows;
    probe_cost = proc_cost * outer_path->parent->rows;

    workspace->startup_cost = hash_cost + inner_path->total_cost; /* this is the total cost for building the hash table */
    workspace->total_cost = workspace->startup_cost + probe_cost + outer_path->total_cost;

    if (!enable_hashjoin)
    {
        workspace->startup_cost += disable_cost;
        workspace->total_cost += disable_cost;
    }
}


void
cout_final_cost_hashjoin(PlannerInfo *root, HashPath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra)
{
    JoinPath *jpath;
    Cost hash_cost, probe_cost;

    if (prev_final_cost_hashjoin_hook)
        (*prev_final_cost_hashjoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_hashjoin(root, path, workspace, extra);

    jpath = &(path->jpath);
    hash_cost = proc_cost * jpath->innerjoinpath->parent->rows;
    probe_cost = proc_cost * jpath->outerjoinpath->parent->rows;

    jpath->path.startup_cost = hash_cost + jpath->innerjoinpath->total_cost;
    jpath->path.total_cost = jpath->path.startup_cost + probe_cost + jpath->outerjoinpath->total_cost;

    if (!enable_hashjoin)
    {
        jpath->path.startup_cost += disable_cost;
        jpath->path.total_cost += disable_cost;
    }
}

void
cout_initial_cost_mergejoin(PlannerInfo *root,
                            JoinCostWorkspace *workspace,
                            JoinType jointype,
                            List *mergeclauses,
                            Path *outer_path, Path *inner_path,
                            List *outersortkeys, List *innersortkeys,
                            #if PG_VERSION_NUM >= 180000
                            int outer_presorted_keys,
                            #endif
                            JoinPathExtraData *extra)
{
    Cost startup_cost, total_cost;

    if (prev_initial_cost_mergejoin_hook)
        (*prev_initial_cost_mergejoin_hook)(root,
                                            workspace,
                                            jointype,
                                            mergeclauses,
                                            outer_path, inner_path,
                                            outersortkeys, innersortkeys,
                                            #if PG_VERSION_NUM >= 180000
                                            outer_presorted_keys,
                                            #endif
                                            extra);
    else
        standard_initial_cost_mergejoin(root,
                                        workspace,
                                        jointype,
                                        mergeclauses,
                                        outer_path, inner_path,
                                        outersortkeys, innersortkeys,
                                        #if PG_VERSION_NUM >= 180000
                                        outer_presorted_keys,
                                        #endif
                                        extra);

    startup_cost = total_cost = 0.0;
    if (IsA(outer_path, SortPath))
    {
        startup_cost += outer_path->total_cost;
        total_cost += outer_path->total_cost;
    }
    else
    {
        startup_cost += outer_path->startup_cost;
        total_cost += outer_path->total_cost;
    }

    if (IsA(inner_path, SortPath))
    {
        startup_cost += inner_path->total_cost;
        total_cost += inner_path->total_cost;
    }
    else
    {
        startup_cost += inner_path->startup_cost;
        total_cost += inner_path->total_cost;
    }

    workspace->startup_cost = startup_cost;
    workspace->total_cost = total_cost + outer_path->rows + inner_path->rows;

    if (!enable_mergejoin)
    {
        workspace->startup_cost += disable_cost;
        workspace->total_cost += disable_cost;
    }
}

void
cout_final_cost_mergejoin(PlannerInfo *root, MergePath *path, JoinCostWorkspace *workspace, JoinPathExtraData *extra)
{
    JoinPath *jpath;
    Cost startup_cost, total_cost;

    if (prev_final_cost_mergejoin_hook)
        (*prev_final_cost_mergejoin_hook)(root, path, workspace, extra);
    else
        standard_final_cost_mergejoin(root, path, workspace, extra);

    jpath = &(path->jpath);
    if (IsA(jpath->outerjoinpath, SortPath))
    {
        startup_cost = jpath->outerjoinpath->total_cost;
        total_cost = jpath->outerjoinpath->total_cost;
    }
    else
    {
        startup_cost = jpath->outerjoinpath->startup_cost;
        total_cost = jpath->outerjoinpath->total_cost;
    }

    if (IsA(jpath->innerjoinpath, SortPath))
    {
        startup_cost += jpath->innerjoinpath->total_cost;
        total_cost += jpath->innerjoinpath->total_cost;
    }
    else
    {
        startup_cost += jpath->innerjoinpath->startup_cost;
        total_cost += jpath->innerjoinpath->total_cost;
    }

    jpath->path.startup_cost = startup_cost;
    jpath->path.total_cost = total_cost + jpath->outerjoinpath->rows + jpath->innerjoinpath->rows;

    if (!enable_mergejoin)
    {
        jpath->path.startup_cost += disable_cost;
        jpath->path.total_cost += disable_cost;
    }
}

void
cout_cost_sort(Path *path, PlannerInfo *root,
               List *pathkeys,
               #if PG_VERSION_NUM >= 180000
               int input_disabled_nodes,
               #endif
               Cost input_cost, double tuples, int width,
			   Cost comparison_cost, int sort_mem,
			   double limit_tuples)
{
    if (prev_cost_sort_hook)
        (*prev_cost_sort_hook)(path, root,
                               pathkeys,
                               #if PG_VERSION_NUM >= 180000
                               input_disabled_nodes,
                               #endif
                               input_cost, tuples, width,
                               comparison_cost, sort_mem,
                               limit_tuples);
    else
        standard_cost_sort(path, root,
                           pathkeys,
                           #if PG_VERSION_NUM >= 180000
                           input_disabled_nodes,
                           #endif
                           input_cost, tuples, width,
                           comparison_cost, sort_mem,
                           limit_tuples);

    path->startup_cost = tuples * log10(tuples) + input_cost;
    path->total_cost = path->startup_cost;

    if (!enable_sort)
    {
        path->startup_cost += disable_cost;
        path->total_cost += disable_cost;
    }
}

void
cout_cost_incremental_sort(Path *path,
                           PlannerInfo *root, List *pathkeys, int presorted_keys,
                           #if PG_VERSION_NUM >= 180000
                           int input_disabled_nodes,
                           #endif
						   Cost input_startup_cost, Cost input_total_cost,
						   double input_tuples, int width, Cost comparison_cost, int sort_mem,
						   double limit_tuples)
{
    if (prev_cost_incremental_sort_hook)
        (*prev_cost_incremental_sort_hook)(path,
                                           root, pathkeys, presorted_keys,
                                           #if PG_VERSION_NUM >= 180000
                                           input_disabled_nodes,
                                           #endif
                                           input_startup_cost, input_total_cost,
                                           input_tuples, width, comparison_cost, sort_mem,
                                           limit_tuples);
    else
        standard_cost_incremental_sort(path,
                                       root, pathkeys, presorted_keys,
                                       #if PG_VERSION_NUM >= 180000
                                       input_disabled_nodes,
                                       #endif
                                       input_startup_cost, input_total_cost,
                                       input_tuples, width, comparison_cost, sort_mem,
                                       limit_tuples);

    path->startup_cost = input_tuples * log10(input_tuples) + input_total_cost;
    path->total_cost = path->startup_cost;

    if (!enable_incremental_sort)
    {
        path->startup_cost += disable_cost;
        path->total_cost += disable_cost;
    }
}

void
cout_cost_materialize(Path *path,
                      #if PG_VERSION_NUM >= 180000
                      int input_disabled_nodes,
                      #endif
                      Cost input_startup_cost, Cost input_total_cost,
                      double tuples, int width)
{
    if (prev_cost_material_hook)
        (*prev_cost_material_hook)(path,
                                   #if PG_VERSION_NUM >= 180000
                                   input_disabled_nodes,
                                   #endif
                                   input_startup_cost, input_total_cost,
                                   tuples, width);
    else
        standard_cost_material(path,
                               #if PG_VERSION_NUM >= 180000
                               input_disabled_nodes,
                               #endif
                               input_startup_cost, input_total_cost,
                               tuples, width);

    path->startup_cost = input_startup_cost;
    path->total_cost = input_total_cost + tuples;

    if (!enable_material)
    {
        path->startup_cost += disable_cost;
        path->total_cost += disable_cost;
    }
}

void
cout_cost_memoize(PlannerInfo *root, MemoizePath *mpath, Cost *rescan_startup_cost, Cost *rescan_total_cost)
{
    Path *subpath;
    Cardinality rows;
    Cost reuse_cost;

    if (prev_cost_memoize_rescan_hook)
        (*prev_cost_memoize_rescan_hook)(root, mpath, rescan_startup_cost, rescan_total_cost);
    else
        standard_cost_memoize_rescan(root, mpath, rescan_startup_cost, rescan_total_cost);

    subpath = mpath->subpath;
    rows = subpath->rows;
    reuse_cost = 2 * proc_cost * (rows - sqrt(rows) / rows) * subpath->total_cost;

    *rescan_startup_cost = subpath->startup_cost;
    *rescan_total_cost = reuse_cost + *rescan_startup_cost;

    if (!enable_memoize)
    {
        *rescan_startup_cost += disable_cost;
        *rescan_total_cost += disable_cost;
    }
}


void
SetCoutStarCostModel()
{
    prev_cost_seqscan_hook = cost_seqscan_hook;
    cost_seqscan_hook = cout_cost_seqscan;

    prev_cost_index_hook = cost_index_hook;
    cost_index_hook = cout_cost_idxscan;

    prev_cost_bitmap_heap_scan_hook = cost_bitmap_heap_scan_hook;
    cost_bitmap_heap_scan_hook = cout_cost_bitmap_scan;

    prev_initial_cost_nestloop_hook = initial_cost_nestloop_hook;
    initial_cost_nestloop_hook = cout_initial_cost_nlj;
    prev_final_cost_nestloop_hook = final_cost_nestloop_hook;
    final_cost_nestloop_hook = cout_final_cost_nlj;

    prev_initial_cost_hashjoin_hook = initial_cost_hashjoin_hook;
    initial_cost_hashjoin_hook = cout_initial_cost_hashjoin;
    prev_final_cost_hashjoin_hook = final_cost_hashjoin_hook;
    final_cost_hashjoin_hook = cout_final_cost_hashjoin;

    prev_initial_cost_mergejoin_hook = initial_cost_mergejoin_hook;
    initial_cost_mergejoin_hook = cout_initial_cost_mergejoin;
    prev_final_cost_mergejoin_hook = final_cost_mergejoin_hook;
    final_cost_mergejoin_hook = cout_final_cost_mergejoin;

    prev_cost_sort_hook = cost_sort_hook;
    cost_sort_hook = cout_cost_sort;
    prev_cost_incremental_sort_hook = cost_incremental_sort_hook;
    cost_incremental_sort_hook = cout_cost_incremental_sort;

    prev_cost_material_hook = cost_material_hook;
    cost_material_hook = cout_cost_materialize;

    prev_cost_memoize_rescan_hook = cost_memoize_rescan_hook;
    cost_memoize_rescan_hook = cout_cost_memoize;
}

void ResetCostModel()
{
    cost_seqscan_hook = prev_cost_seqscan_hook;
    prev_cost_seqscan_hook = NULL;

    cost_index_hook = prev_cost_index_hook;
    prev_cost_index_hook = NULL;

    cost_bitmap_heap_scan_hook = prev_cost_bitmap_heap_scan_hook;
    prev_cost_bitmap_heap_scan_hook = NULL;

    initial_cost_nestloop_hook = prev_initial_cost_nestloop_hook;
    prev_initial_cost_nestloop_hook = NULL;
    final_cost_nestloop_hook = prev_final_cost_nestloop_hook;
    prev_final_cost_nestloop_hook = NULL;

    initial_cost_hashjoin_hook = prev_initial_cost_hashjoin_hook;
    prev_initial_cost_hashjoin_hook = NULL;
    final_cost_hashjoin_hook = prev_final_cost_hashjoin_hook;
    prev_final_cost_hashjoin_hook = NULL;

    initial_cost_mergejoin_hook = prev_initial_cost_mergejoin_hook;
    prev_initial_cost_mergejoin_hook = NULL;
    final_cost_mergejoin_hook = prev_final_cost_mergejoin_hook;
    prev_final_cost_mergejoin_hook = NULL;

    cost_sort_hook = prev_cost_sort_hook;
    prev_cost_sort_hook = NULL;
    cost_incremental_sort_hook = prev_cost_incremental_sort_hook;
    prev_cost_incremental_sort_hook = NULL;

    cost_material_hook = prev_cost_material_hook;
    prev_cost_material_hook = NULL;

    cost_memoize_rescan_hook = prev_cost_memoize_rescan_hook;
    prev_cost_memoize_rescan_hook = NULL;
}

void
toggle_cost_model(bool newval, void *extra)
{
    if (newval)
        SetCoutStarCostModel();
    else
        ResetCostModel();
}

void _PG_init(void)
{
    DefineCustomBoolVariable("enable_cout", "Enable the Cout* cost model", NULL,
                             &cm_enabled, false,
                             PGC_USERSET, 0,
                             NULL,
                             toggle_cost_model,
                             NULL);

    DefineCustomRealVariable("cout_scan_cost", "Cost of scanning a tuple (i.e. sequential I/O cost)", NULL,
                             &scan_cost, 1.0,
                             0.0, 1.0e11,
                             PGC_USERSET, 0,
                             NULL,
                             NULL,
                             NULL);
    DefineCustomRealVariable("cout_ind_cost", "Cost of processing an index tuple (i.e. random I/O cost)", NULL,
                            &ind_cost, 2.0,
                            0.0, 1.0e11,
                            PGC_USERSET, 0,
                            NULL,
                            NULL,
                            NULL);
    DefineCustomRealVariable("cout_proc_cost", "Cost of processing a tuple (e.g. hash functions, etc.)", NULL,
                            &proc_cost, 1.2,
                            0.0, 1.0e11,
                            PGC_USERSET, 0,
                            NULL,
                            NULL,
                            NULL);
}

void
_PG_fini(void)
{
    ResetCostModel();
}
