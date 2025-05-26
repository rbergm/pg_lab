
#include <math.h>
#include <unordered_map>
#include <vector>

#include "antlr4-runtime.h"
#include "HintBlockLexer.h"
#include "HintBlockParser.h"
#include "HintBlockBaseListener.h"

extern "C" {

/* Postgres server includes */
#include "postgres.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"

/* Own includes */
#include "hints.h"
}


class HintBlockListener : public pg_lab::HintBlockBaseListener
{
    public:
        explicit HintBlockListener(PlannerInfo *root, PlannerHints *hints)
            : root_(root), hints_(hints), local_parallel_hint_(false)
        {
            rti_cache_ = std::unordered_map<std::string, Index>(root->simple_rel_array_size);
        }

        void enterPlan_mode_setting(pg_lab::HintBlockParser::Plan_mode_settingContext *ctx) override
        {
            if (ctx->FULL())
                hints_->mode = FULL;
            else if (ctx->ANCHORED())
                hints_->mode = ANCHORED;
            else
                elog(ERROR, "Unknown plan mode setting: %s", ctx->getText().c_str());
        }

        void enterParallelization_setting(pg_lab::HintBlockParser::Parallelization_settingContext *ctx) override
        {
            if (local_parallel_hint_)
            {
                ereport(WARNING,
                    errmsg("[pg_lab] Ignoring global parallelization setting"),
                    errdetail("Global parallelization setting is overridden by local hint"));
                return;
            }

            if (ctx->PARALLEL())
                hints_->parallel_mode = PARMODE_PARALLEL;
            else if (ctx->SEQUENTIAL())
                hints_->parallel_mode = PARMODE_SEQUENTIAL;
            else if (ctx->DEFAULT())
                hints_->parallel_mode = PARMODE_DEFAULT;
            else
                ereport(ERROR, errmsg("[pg_lab] Unknown parallelization mode setting: %s", ctx->getText().c_str()));
        }

        void enterJoin_order_hint(pg_lab::HintBlockParser::Join_order_hintContext *ctx) override
        {
            auto join_order = this->ParseJoinOrder(ctx->join_order_entry());
            hints_->join_order_hint = join_order;
        }

        void enterJoin_op_hint(pg_lab::HintBlockParser::Join_op_hintContext *ctx) override
        {
            PhysicalOperator op = OP_UNKNOWN;
            Relids relations    = EMPTY_BITMAP;
            for (const auto &relname : ctx->binary_rel_id()->relation_id())
            {
                Index rt_index = ResolveRTIndex(relname->getText());
                relations = bms_add_member(relations, rt_index);
            }

            for (const auto &relname : ctx->relation_id())
            {
                Index rt_index = ResolveRTIndex(relname->getText());
                relations = bms_add_member(relations, rt_index);
            }

            if (ctx->NESTLOOP())
                op = OP_NESTLOOP;
            else if (ctx->HASHJOIN())
                op = OP_HASHJOIN;
            else if (ctx->MERGEJOIN())
                op = OP_MERGEJOIN;
            else if (ctx->MEMOIZE())
                op = OP_MEMOIZE;
            else if (ctx->MATERIALIZE())
                op = OP_MATERIALIZE;
            else
                ereport(ERROR, errmsg("[pg_lab] Unknown join operator: %s", ctx->getText().c_str()));

            MakeOperatorHint(relations, op, ctx->param_list());
        }

        void enterScan_op_hint(pg_lab::HintBlockParser::Scan_op_hintContext *ctx) override {
            PhysicalOperator op = OP_UNKNOWN;
            auto relname = ctx->relation_id()->getText().c_str();
            Index rt_index = ResolveRTIndex(relname);
            Relids baserel = (Relids) bms_make_singleton(rt_index);

            if (ctx->SEQSCAN())
                op = OP_SEQSCAN;
            else if (ctx->IDXSCAN())
                op = OP_IDXSCAN;
            else if (ctx->BITMAPSCAN())
                op = OP_BITMAPSCAN;
            else if (ctx->MEMOIZE())
                op = OP_MEMOIZE;
            else if (ctx->MATERIALIZE())
                op = OP_MATERIALIZE;
            else
                ereport(ERROR, errmsg("[pg_lab] Unknown scan operator: %s", ctx->getText().c_str()));


            MakeOperatorHint(baserel, op, ctx->param_list());
        }

        void enterResult_hint(pg_lab::HintBlockParser::Result_hintContext *ctx) override
        {
            if (!ctx->parallel_hint())
            {
                ereport(WARNING, errmsg("[pg_lab] Ignoring empty Result hint"));
                return;
            }

            if (local_parallel_hint_)
            {
                ereport(WARNING,
                    errmsg("[pg_lab] Found multiple parallel hints"),
                    errdetail("Postgres normally creates only one parallel subplan. This might break the query."));
            }

            local_parallel_hint_ = true;

            auto parallel_hint = ctx->parallel_hint();
            if (!parallel_hint->INT())
            {
                ereport(ERROR,
                    errmsg("[pg_lab] Invalid parallel hint format: '%s'", parallel_hint->getText().c_str()));
                return;
            }
            hints_->parallel_workers = std::atoi(parallel_hint->INT()->getText().c_str());
            hints_->parallelize_entire_plan = true;
            hints_->parallel_mode = PARMODE_PARALLEL;
        }

        void enterCardinality_hint(pg_lab::HintBlockParser::Cardinality_hintContext *ctx) override
        {
            Relids relations = EMPTY_BITMAP;
            for (const auto &relname : ctx->relation_id())
            {
                Index rt_index = ResolveRTIndex(relname->getText());
                relations = bms_add_member(relations, rt_index);
            }
            Cardinality cardinality = std::atof(ctx->INT()->getText().c_str());

            CardinalityHint *cardinality_hint = (CardinalityHint*) hash_search(hints_->cardinality_hints,
                                                                               &relations, HASH_ENTER, NULL);
            cardinality_hint->card = cardinality;
        }

    private:
        PlannerInfo  *root_;
        PlannerHints *hints_;

        std::unordered_map<std::string, Index> rti_cache_;
        bool local_parallel_hint_ = false;

        Index ResolveRTIndex(std::string relname)
        {
            auto cached = rti_cache_.find(relname);
            if (cached != rti_cache_.end())
                return cached->second;

            RangeTblEntry *rte = nullptr;
            bool found         = false;
            Index relid;

            for (int i = 1; i < root_->simple_rel_array_size; ++i)
            {
                rte = root_->simple_rte_array[i];
                if (rte == NULL
                    || rte->rtekind != RTE_RELATION
                    || !rte->eref->aliasname)
                    continue;

                if (relname == rte->eref->aliasname)
                {
                    relid = root_->simple_rel_array[i]->relid;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                ereport(ERROR,
                    errmsg("[pg_lab] Relation %s not found in the range table", relname.c_str()),
                    errdetail("Perhaps it's a typo or it refers to a temporary relation (CTE, subquery, ...)?"));
                return InvalidIndex;
            }

            rti_cache_[relname] = relid;
            return relid;
        }

        void MakeOperatorHint(Relids relations, PhysicalOperator op, pg_lab::HintBlockParser::Param_listContext *ctx)
        {
            bool hint_found;
            OperatorHint *operator_hint = (OperatorHint*) hash_search(hints_->operator_hints,
                                                                      &relations, HASH_ENTER, &hint_found);

            if (!hint_found) {
                operator_hint->op = OP_UNKNOWN;
                operator_hint->materialize_output = false;
                operator_hint->memoize_output = false;
            }

            if (op == OP_MEMOIZE)
            {
                operator_hint->memoize_output = true;
                return;
            }
            else if (op == OP_MATERIALIZE)
            {
                operator_hint->materialize_output = true;
                return;
            }
            else
                operator_hint->op = op;

            if (!ctx || !ctx->parallel_hint().size())
                return;
            if (local_parallel_hint_)
            {
                ereport(ERROR,
                    errmsg("[pg_lab] Found multiple parallel hints"),
                    errdetail("Postgres only supports one parallel subplan."));
            }

            local_parallel_hint_ = true;

            auto parallel_hints = ctx->parallel_hint();
            if (parallel_hints.size() > 1)
                ereport(WARNING, errmsg("[pg_lab] Multiple parallel hints found. Using the last one."));

            auto parallel_hint = parallel_hints.back();
            if (!parallel_hint->INT())
            {
                ereport(ERROR,
                    errmsg("[pg_lab] Invalid parallel hint format: '%s'", parallel_hint->getText().c_str()));
                return;
            }

            int workers = std::atoi(parallel_hint->INT()->getText().c_str());
            operator_hint->parallel_workers = workers;
            hints_->parallel_rels = relations;
            hints_->parallel_mode = PARMODE_PARALLEL;
            hints_->parallel_workers = workers;
        }

        void MakeCostHint(std::vector<pg_lab::HintBlockParser::Cost_hintContext*> ctxs, Relids relations, PhysicalOperator op)
        {
            CostHint *cost_hint;
            bool      hint_found;
            bool      is_baserel;

            if (ctxs.size() > 1)
            {
                ereport(WARNING, errmsg("[pg_lab] Multiple cost hints found. Using the last one."));
                return;
            }
            auto ctx = ctxs.back();

            is_baserel = bms_singleton_member(relations);
            cost_hint  = (CostHint *) hash_search(hints_->cost_hints, &relations, HASH_ENTER, &hint_found);

            /* If we encoutered a new hint, we need to initialize its costs */
            if (!hint_found && is_baserel)
            {
                cost_hint->node_type = BASE_REL;
                cost_hint->costs.scan_cost.seqscan_startup = NAN;
                cost_hint->costs.scan_cost.seqscan_total = NAN;
                cost_hint->costs.scan_cost.idxscan_startup = NAN;
                cost_hint->costs.scan_cost.idxscan_total = NAN;
                cost_hint->costs.scan_cost.bitmap_startup = NAN;
                cost_hint->costs.scan_cost.bitmap_total = NAN;
            }
            else if (!hint_found && !is_baserel)
            {
                cost_hint->node_type = JOIN_REL;
                cost_hint->costs.join_cost.nestloop_startup = NAN;
                cost_hint->costs.join_cost.nestloop_total = NAN;
                cost_hint->costs.join_cost.hash_startup = NAN;
                cost_hint->costs.join_cost.hash_total = NAN;
                cost_hint->costs.join_cost.merge_startup = NAN;
                cost_hint->costs.join_cost.merge_total = NAN;
            }

            auto startup_cost = ParseCost(ctx->cost().at(0));
            auto total_cost   = ParseCost(ctx->cost().at(1));

            switch (op)
            {
                case OP_SEQSCAN:
                    cost_hint->costs.scan_cost.seqscan_startup = startup_cost;
                    cost_hint->costs.scan_cost.seqscan_total = total_cost;
                    break;
                case OP_IDXSCAN:
                    cost_hint->costs.scan_cost.idxscan_startup = startup_cost;
                    cost_hint->costs.scan_cost.idxscan_total = total_cost;
                    break;
                case OP_BITMAPSCAN:
                    cost_hint->costs.scan_cost.bitmap_startup = startup_cost;
                    cost_hint->costs.scan_cost.bitmap_total = total_cost;
                    break;
                case OP_NESTLOOP:
                    cost_hint->costs.join_cost.nestloop_startup = startup_cost;
                    cost_hint->costs.join_cost.nestloop_total = total_cost;
                    break;
                case OP_HASHJOIN:
                    cost_hint->costs.join_cost.hash_startup = startup_cost;
                    cost_hint->costs.join_cost.hash_total = total_cost;
                    break;
                case OP_MERGEJOIN:
                    cost_hint->costs.join_cost.merge_startup = startup_cost;
                    cost_hint->costs.join_cost.merge_total = total_cost;
                    break;
                default:
                    elog(ERROR, "Unknown scan operator: %d", op);
                    break;
            }
        }

        Cost ParseCost(pg_lab::HintBlockParser::CostContext *ctx)
        {
            if (ctx->INT())
                return std::atoi(ctx->INT()->getText().c_str());
            else if (ctx->FLOAT())
                return std::atof(ctx->FLOAT()->getText().c_str());
            else
            {
                ereport(ERROR, errmsg("[pg_lab] Unknown cost format: '%s'", ctx->getText().c_str()));
                return -1;
            }
        }

        JoinOrder *ParseJoinOrder(pg_lab::HintBlockParser::Join_order_entryContext *ctx)
        {
            auto base_join_order = ctx->base_join_order();
            if (base_join_order)
                return ParseJoinOrderBase(base_join_order);
            else
                return ParseJoinOrderIntermediate(ctx->intermediate_join_order());
        }

        JoinOrder *ParseJoinOrderIntermediate(pg_lab::HintBlockParser::Intermediate_join_orderContext *ctx)
        {
            auto entries = ctx->join_order_entry();
            /* XXX: robustness (array accesses + don't merge nodes with overlapping relations) */
            auto outer_child  = ParseJoinOrder(entries.at(0));
            auto inner_child  = ParseJoinOrder(entries.at(1));
            auto relation_bms = bms_union(outer_child->relids, inner_child->relids);

            JoinOrder *join_order   = (JoinOrder*) palloc0(sizeof(JoinOrder));
            join_order->node_type   = JOIN_REL;
            join_order->relids      = relation_bms;
            join_order->outer_child = outer_child;
            join_order->inner_child = inner_child;

            outer_child->parent_node = join_order;
            inner_child->parent_node = join_order;

            auto current_level = std::max(outer_child->level, inner_child->level);
            join_order->level  = current_level + 1;

            return join_order;
        }

        JoinOrder *ParseJoinOrderBase(pg_lab::HintBlockParser::Base_join_orderContext *ctx)
        {
            auto relname            = ctx->relation_id()->getText();
            Index rt_index          = ResolveRTIndex(relname);
            Bitmapset *relation_bms = bms_make_singleton(rt_index);

            JoinOrder *join_order       = (JoinOrder*) palloc0(sizeof(JoinOrder));
            join_order->node_type       = BASE_REL;
            join_order->relids          = relation_bms;
            join_order->rt_index        = rt_index;
            join_order->base_identifier = pstrdup(relname.c_str());
            join_order->level           = 0;

            return join_order;
        }

};

static void
link_join_order_operator_hints(JoinOrder *join_order, HTAB *operator_hints)
{
    JoinOrderIterator  iterator;
    ListCell          *lc;

    init_join_order_iterator(&iterator, join_order);

    while (!iterator.done)
    {
        foreach(lc, iterator.current_nodes)
        {
            JoinOrder *current_node = (JoinOrder *) lfirst(lc);
            bool hint_found;

            OperatorHint *operator_hint = (OperatorHint *) hash_search(operator_hints, &(current_node->relids),
                                                                       HASH_FIND, &hint_found);
            if (!hint_found)
                continue;

            current_node->physical_op = operator_hint;
        }

        join_order_iterator_next(&iterator);
    }

    free_join_order_iterator(&iterator);
}

static void
determine_parallel_rels(PlannerHints *hints)
{
    if (hints->join_order_hint)
    {
        /*
         * If we have a join order, we just need to find its outer-most relation. Postgres will use the number of parallel
         * workers from this relation to determine the degree of parallelism for the entire plan.
         */

        JoinOrder *parallel_node = traverse_join_order(hints->join_order_hint, hints->parallel_rels);

        while (parallel_node->node_type != BASE_REL)
            parallel_node = parallel_node->outer_child;

        /* don't pfree() the old relids, they are also used in the operator! */
        hints->parallel_rels = parallel_node->relids;
    }
    else
    {
        /*
         * While walking the ANTLR syntax tree, we have already set the parallel_rels to all relations that take part in the
         * parallel subplan. Since we don't have a join order, any one of them could become the outer-most relation in the
         * final query plan (which is the relation that Postgres uses to determine the degree of parallelism).
         * Therefore, we are already done here.
         *
         * For good measure, we issue a warning because all of the base rels will have the parallel workers annotated, which
         * could influence the plan selection (because it influences cost estimation for the access paths).
         */
        ereport(WARNING, errmsg("[pg_lab] Should only set parallel workers if the join order is also specified."));
    }
}


void
init_hints(PlannerInfo *root, PlannerHints *hints)
{
    hints->raw_query     = current_query_string;
    hints->mode          = ANCHORED;
    hints->parallel_mode = PARMODE_DEFAULT;
    hints->parallel_rels = NULL;
    hints->parallelize_entire_plan = false;

    if (!current_query_string)
    {
        hints->contains_hint = false;
        return;
    }

    std::string *query_buffer = new std::string(hints->raw_query);

    auto hint_block_start = query_buffer->find("/*=pg_lab=");
    auto hint_block_end   = query_buffer->find("*/");
    if (hint_block_start == std::string::npos || hint_block_end == std::string::npos)
    {
        hints->contains_hint = false;
        delete query_buffer;
        return;
    }

    hints->contains_hint = true;
    auto hint_string = query_buffer->substr(hint_block_start, hint_block_end - hint_block_start + 2);
    hints->raw_hint = (char*) pstrdup(hint_string.c_str());

    /*
     * Initialize the hash tables.
     * We really need to create multiple separate control structures, because these might get inflated during the hash table
     * creation.
     */
    HASHCTL op_hints_hctl;
    op_hints_hctl.keysize = sizeof(Relids);
    op_hints_hctl.entrysize = sizeof(OperatorHint);
    op_hints_hctl.hcxt = CurrentMemoryContext;
    op_hints_hctl.hash = bitmap_hash;
    op_hints_hctl.match = bitmap_match;
    hints->operator_hints = hash_create("OperatorHintHashes",
                                        16L,
                                        &op_hints_hctl,
                                        HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    HASHCTL card_hints_hctl;
    card_hints_hctl.keysize = sizeof(Relids);
    card_hints_hctl.entrysize = sizeof(CardinalityHint);
    card_hints_hctl.hcxt = CurrentMemoryContext;
    card_hints_hctl.hash = bitmap_hash;
    card_hints_hctl.match = bitmap_match;
    hints->cardinality_hints = hash_create("CardinalityHintHashes",
                                           32L,
                                           &card_hints_hctl,
                                           HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    HASHCTL cost_hints_hctl;
    cost_hints_hctl.keysize = sizeof(Relids);
    cost_hints_hctl.entrysize = sizeof(CostHint);
    cost_hints_hctl.hcxt = CurrentMemoryContext;
    cost_hints_hctl.hash = bitmap_hash;
    cost_hints_hctl.match = bitmap_match;
    hints->cost_hints = hash_create("CostHintHashes",
                                    32L,
                                    &cost_hints_hctl,
                                    HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    /*
     * C++ extensions for Postgres should only put POD-objects onto the stack. This enables correct unrolling in case of
     * errors, thereby keeping the current connection alive. Therefore, we need to allocate all non-POD objects on the heap.
     * Even worse, we are forced to use explicit new/delete syntax here, because smart pointers once again are not PODs.
     *
     * Notice that this code will leak in case of errors because we cannot clean up the allocated memory correctly.
     *
     * XXX: Maybe we can figure something out to prevent leakage.
     */
    antlr4::ANTLRInputStream *parser_input = new antlr4::ANTLRInputStream(hint_string);
    pg_lab::HintBlockLexer *lexer = new pg_lab::HintBlockLexer(parser_input);
    antlr4::CommonTokenStream *tokens = new antlr4::CommonTokenStream(lexer);
    pg_lab::HintBlockParser *parser = new pg_lab::HintBlockParser(tokens);

    antlr4::tree::ParseTree *tree = parser->hint_block();
    HintBlockListener *listener = new HintBlockListener(root, hints);
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(listener, tree);

    delete listener;
    delete parser;
    delete tokens;
    delete lexer;
    delete parser_input;
    delete query_buffer;

    if (hints->join_order_hint && hash_get_num_entries(hints->operator_hints) > 0)
        link_join_order_operator_hints(hints->join_order_hint, hints->operator_hints);

    // if (hints->parallel_workers > 0 && !hints->parallelize_entire_plan)
    //     determine_parallel_rels(hints);
}


void
free_hints(PlannerHints *hints)
{
    if (!hints)
        return;

    if (hints->join_order_hint)
        free_join_order(hints->join_order_hint);

    hash_destroy(hints->operator_hints);
    hash_destroy(hints->cardinality_hints);
    hash_destroy(hints->cost_hints);
}
