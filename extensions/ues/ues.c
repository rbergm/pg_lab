
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_statistic.h"
#include "lib/stringinfo.h"
#include "optimizer/joininfo.h"
#include "optimizer/paths.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC;

extern join_search_hook_type join_search_hook;

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_fini(void);

static bool ues_enabled = false;

typedef double Freq;

static join_search_hook_type prev_join_search_hook = NULL;

RelOptInfo* ues_join_search(PlannerInfo *root, int levels_needed, List *initial_rels);

typedef double UpperBound;

typedef enum KeyType
{
    KT_NONE = 0,
    KT_PRIMARY = 1, /* Primary key or UNIQUE */
    KT_FOREIGN = 2
} KeyType;

typedef struct UesState
{
    RelOptInfo  *current_intermediate;
    UpperBound   current_bound;
    List        *joined_keys;       /* UesJoinKey entries */
    List        *candidate_keys;    /* UesJoinKey entries */
} UesState;

typedef struct UesJoinKey
{
    RelOptInfo  *baserel;
    Var         *join_key;
    Freq         max_freq;
    KeyType      key_type;
} UesJoinKey;


static UesState*
ues_join_search_init(PlannerInfo *root)
{
    UesState *state = palloc0(sizeof(UesState));
    state->current_intermediate = NULL;
    state->current_bound        = 0;
    state->joined_keys          = NIL;
    state->candidate_keys       = NIL;

    root->join_search_private = state;
    return state;
}

static void
ues_join_search_cleanup(PlannerInfo *root)
{
    UesState *state = root->join_search_private;
    list_free_deep(state->joined_keys);
    list_free_deep(state->candidate_keys);
    pfree(state);
    root->join_search_private = NULL;
}

static bool
ues_supported_query(PlannerInfo *root)
{
    /*
     * TODO
     * We probably need an expression_walker to do this properly.
     * Expect some (read: a lot) of backend crashes until we do so.
     */
    return root->parse->commandType == CMD_SELECT &&
        root->ec_merging_done && root->eq_classes != NIL &&
        bms_is_empty(root->outer_join_rels) &&
        root->parse->setOperations == NULL;
}

static void
ues_print_state(PlannerInfo *root, UesState *ues_state)
{
    ListCell    *lc;
    StringInfo   msg = makeStringInfo();

    foreach(lc, ues_state->candidate_keys)
    {
        UesJoinKey  *key = (UesJoinKey *) lfirst(lc);
        Oid          rel = root->simple_rte_array[key->baserel->relid]->relid;

        char *relname = get_rel_name(rel);
        char *attname = get_attname(rel, key->join_key->varattno, false);
        char *keytype = key->key_type == KT_PRIMARY ? " [PK]" : (key->key_type == KT_FOREIGN ? " [FK]" : "");
        appendStringInfo(msg, "\n\t%s.%s: MF=%f%s;", relname, attname, key->max_freq, keytype);
    }

    elog(INFO, "UES state: %s", msg->data);
    destroyStringInfo(msg);
}

static double
fetch_max_column_freq(PlannerInfo *root, RelOptInfo *rel, Var *column)
{
    AttStatsSlot sslot;
    VariableStatData vardata;
    double max_freq;

    examine_variable(root, (Node *) column, 0, &vardata);
    get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_MCV, InvalidOid,
                     ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);

    if (sslot.nvalues > 0)
    {
        /* We found an MCV list, use it */
        max_freq = sslot.numbers[0] * rel->tuples; /* index 0 is the highest frequency */
    }
    else
    {
        /* No MCV found, assume equal distribution */
        bool default_est; /* ignored, keep compiler quiet */
        double ndistinct = get_variable_numdistinct(&vardata, &default_est);

        if (ndistinct < 0) /* negative means ND is a fraction of the table size, unwrap it */
            ndistinct *= -1.0 * rel->tuples;
        max_freq = rel->tuples / ndistinct;
    }

    ReleaseVariableStats(vardata);
    return max_freq;
}

static void
ues_set_baserel_freqs(PlannerInfo *root, List **rels)
{
    ListCell    *relc,
                *ecc;
    UesState    *ues_state = (UesState*) root->join_search_private;

    foreach(relc, *rels)
    {
        RelOptInfo  *baserel = lfirst(relc);
        int          i = -1;

        if (!baserel->has_eclass_joins)
            ereport(ERROR, (errcode(ERRCODE_ASSERT_FAILURE),
                errmsg("Cross join not supported for UES. This should never be called.")));

        while ((i = bms_next_member(baserel->eclass_indexes, i)) >= 0)
        {
            EquivalenceClass    *eqc = (EquivalenceClass*) list_nth(root->eq_classes, i);
            EquivalenceMember   *em = NULL;
            UesJoinKey          *key = NULL;
            AttStatsSlot         sslot;
            VariableStatData     vardata;
            Freq                 max_freq;
            KeyType              key_type = KT_NONE;

            if (eqc->ec_has_const)
                continue;

            foreach(ecc, eqc->ec_members)
            {
                em = (EquivalenceMember *) lfirst(ecc);
                if (bms_equal(em->em_relids, baserel->relids))
                    break;
            }

            Assert(em != NULL);

            Assert(IsA(em->em_expr, Var));
            examine_variable(root, (Node *) em->em_expr, 0, &vardata);
            get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_MCV, InvalidOid,
                             ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);

            if (sslot.nvalues > 0)
            {
                /* We found an MCV list, use it */
                max_freq = sslot.numbers[0] * baserel->tuples; /* index 0 is the highest frequency */
            }
            else
            {
                /* No MCV found, assume equal distribution */

                bool default_est; /* we don't care how PG came up with the estimate, we just use it */
                double ndistinct = get_variable_numdistinct(&vardata, &default_est);

                if (ndistinct < 0) /* negative means ND is a fraction of the table size, unwrap it */
                    ndistinct *= -1.0 * baserel->tuples;
                max_freq = baserel->tuples / ndistinct;
            }

            if (vardata.isunique)
            {
                key_type = KT_PRIMARY;
            }
            else if (root->fkey_list != NIL)
            {
                ListCell *fkc;
                foreach(fkc, root->fkey_list)
                {
                    ForeignKeyOptInfo *fkey = (ForeignKeyOptInfo *) lfirst(fkc);
                    if (fkey->con_relid == baserel->relid &&
                        fkey->nkeys == 1 &&
                        fkey->conkey[0] == ((Var *) em->em_expr)->varattno)
                    {
                        key_type = KT_FOREIGN;
                        break;
                    }
                }
            }

            ReleaseVariableStats(vardata);

            key = (UesJoinKey *) palloc0(sizeof(UesJoinKey));
            key->baserel = baserel;
            key->join_key = (Var *) em->em_expr;
            key->max_freq = max_freq;
            key->key_type = key_type;

            ues_state->candidate_keys = lappend(ues_state->candidate_keys, key);
        }

    }
}

/*
 * XXX
 *
 * We return the index of the best candidate rather than the candidate itself to enable easy deletion from the list of
 * candidates later on.
 *
 * By convention, the index should be -1 (or any negative number for that matter).
 */
static bool
ues_get_best_join_partner(PlannerInfo *root, UesJoinKey *outer_key, UpperBound *bound, int *best_candidate_idx)
{
    UesState     *ues_state = (UesState *) root->join_search_private;
    List        **joinrels  = root->join_rel_level;
    RelOptInfo   *outer_rel = ues_state->current_intermediate;
    UpperBound    min_bound = __DBL_MAX__;
    ListCell     *lc;

    foreach (lc, ues_state->candidate_keys)
    {
        UesJoinKey          *inner_key = (UesJoinKey*) lfirst(lc);
        RelOptInfo          *inner_rel = inner_key->baserel;
        EquivalenceClass    *ec;
        ListCell            *lc2;

        if (!have_relevant_joinclause(root, outer_rel, inner_rel))
            continue;

        foreach (lc2, root->eq_classes)
        {
            ec = (EquivalenceClass *) lfirst(lc2);
            if (find_ec_member_matching_expr(ec, inner_key->join_key, NULL))
                break;
        }

        Assert(ec != NULL);

        foreach (lc2, ues_state->joined_keys)
        {
           UesJoinKey   *outer_key = (UesJoinKey *) lfirst(lc2);
           UpperBound    current_bound;
           if (!find_ec_member_matching_expr(ec, outer_key->join_key, NULL))
               continue;

            /*
             * We found a valid join predicate between the current intermediate
             * and our new join candidate. Hooray!
             * Now, let's see if this predicate is any good.
             */
            current_bound = min(ues_state->current_bound / outer_key->max_freq, inner_rel->rows / inner_key->max_freq)
                            * inner_key->max_freq
                            * outer_key->max_freq;

            /*
             * XXX
             * We should also handle the other special join types here (e.g. PK/PK join)
             */
            if (outer_key->key_type == KT_PRIMARY && inner_key->key_type == KT_FOREIGN)
            {
                current_bound = min(current_bound, inner_rel->rows);
            }

            if (current_bound < min_bound)
            {
                min_bound = current_bound;
                best_candidate_idx = foreach_current_index(lc2);
            }
        }
    }

    return best_candidate_idx >= 0;
}


static void
ues_select_initial_rel(PlannerInfo *root, UesState *ues_state)
{
    UpperBound   min_bound = __DBL_MAX__;
    int          outer_idx = -1, inner_idx = -1;
    RelOptInfo  *result_rel;
    ListCell    *lc;

    foreach (lc, ues_state->candidate_keys)
    {
        UesJoinKey *outer_key = (UesJoinKey *) lfirst(lc);
        int         current_inner_idx = -1;
        UpperBound  current_bound;

        if (!ues_get_best_join_partner(root, outer_key, &current_bound, &current_inner_idx))
            continue;

        if (current_bound < min_bound)
        {
            min_bound = current_bound;
            outer_idx = foreach_current_index(lc);
            inner_idx = current_inner_idx;
        }
    }
    Assert(outer_idx >= 0 && inner_idx >= 0);

    /*
     * TODO
     * Make the actual rel and update the UesState accordingly.
     */

    ues_state->current_intermediate = result_rel;
}

static void
ues_join_search_one_level(PlannerInfo *root, int level)
{
    UesState     *ues_state         = (UesState *) root->join_search_private;
    List        **joinrels          = root->join_rel_level;
    RelOptInfo   *outer_rel         = ues_state->current_intermediate;
    UpperBound    min_bound         = __DBL_MAX__;
    RelOptInfo   *best_candidate    = NULL;
    RelOptInfo   *result_rel;
    ListCell     *lc;

    foreach (lc, ues_state->candidate_keys)
    {
        UesJoinKey          *inner_key = (UesJoinKey*) lfirst(lc);
        RelOptInfo          *inner_rel = inner_key->baserel;
        EquivalenceClass    *ec;
        ListCell            *lc2;

        if (!have_relevant_joinclause(root, outer_rel, inner_rel))
            continue;

        foreach (lc2, root->eq_classes)
        {
            ec = (EquivalenceClass *) lfirst(lc2);
            if (find_ec_member_matching_expr(ec, inner_key->join_key, NULL))
                break;
        }

        Assert(ec != NULL);

        foreach (lc2, ues_state->joined_keys)
        {
           UesJoinKey   *outer_key = (UesJoinKey *) lfirst(lc2);
           UpperBound    current_bound;
           if (!find_ec_member_matching_expr(ec, outer_key->join_key, NULL))
               continue;

            /*
             * We found a valid join predicate between the current intermediate
             * and our new join candidate. Hooray!
             * Now, let's see if this predicate is any good.
             */
            current_bound = min(ues_state->current_bound / outer_key->max_freq, inner_rel->rows / inner_key->max_freq)
                            * inner_key->max_freq
                            * outer_key->max_freq;

            /*
             * XXX
             * We should also handle the other special join types here (e.g. PK/PK join)
             */
            if (outer_key->key_type == KT_PRIMARY && inner_key->key_type == KT_FOREIGN)
            {
                current_bound = min(current_bound, inner_rel->rows);
            }

            if (current_bound < min_bound)
            {
                min_bound       = current_bound;
                best_candidate  = inner_rel;
            }
        }

    }

    Assert(best_candidate != NULL);

    /*
     * XXX
     * We need to find an elegant way to compute whether we should introduce a bushy
     * node based on a candidate FK join.
     * For now, we just focus on linear join orders.
     */

    result_rel = make_join_rel(root, outer_rel, best_candidate);

    /*
     * TODO
     * Create a hash path based on the cheapest paths to outer_rel and best_candidate.
     */

    /*
     * TODO
     * Update join_keys and candidate_keys and their frequencies and key types.
     */

     ues_state->current_intermediate = result_rel;
     ues_state->current_bound = min_bound;
}

RelOptInfo*
ues_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    int          lev;
    RelOptInfo  *rel;
    UesState    *ues_state;
    bool         triggers_ues = ues_enabled && ues_supported_query(root);

    elog(INFO, "UES join search: %s", triggers_ues ? "enabled" : "disabled");

    if (!triggers_ues && prev_join_search_hook)
        return prev_join_search_hook(root, levels_needed, initial_rels);
    if (!triggers_ues)
        return standard_join_search(root, levels_needed, initial_rels);

    ues_state = ues_join_search_init(root);
    ues_set_baserel_freqs(root, &initial_rels);
    ues_print_state(root, (UesState *) root->join_search_private);

    /*
     * XXX
     * Initialize by selecting the smallest base relation as the first
     */

    lev = 2;
    while (!bms_equal(root->all_baserels, ues_state->current_intermediate->relids))
    {
        ues_join_search_one_level(root, lev++);

        /*
         * XXX
         * For now, we do not deal with parallelization b/c the large cardinality overestimation introduced by the UES upper
         * bound leads to a strong preference of heavily heavily parallelized plans. In actuality when executing the query we
         * would end up with way too many parallel workers. In turn, this slows down the query by a lot.
         */
    }

    rel = ues_state->current_intermediate;
    ues_join_search_cleanup(root);
    return rel;
}

void _PG_init(void)
{
    prev_join_search_hook = join_search_hook;
    join_search_hook = ues_join_search;

    DefineCustomBoolVariable("ues.enable_ues", "Enable the UES query optimizer", NULL,
                             &ues_enabled, false,
                             PGC_USERSET, 0, NULL, NULL, NULL);
}
