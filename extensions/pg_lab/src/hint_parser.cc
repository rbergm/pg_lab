
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
            : root_(root), hints_(hints), temp_gucs_() {}

        void enterPlan_mode_setting(pg_lab::HintBlockParser::Plan_mode_settingContext *ctx) override
        {
            if (ctx->FULL())
                hints_->mode = HINTMODE_FULL;
            else if (ctx->ANCHORED())
                hints_->mode = HINTMODE_ANCHORED;
            else
                elog(ERROR, "Unknown plan mode setting: %s", ctx->getText().c_str());

            hints_->contains_hint = true;
        }

        void enterParallelization_setting(pg_lab::HintBlockParser::Parallelization_settingContext *ctx) override
        {
            if (hints_->parallel_rels || hints_->parallelize_entire_plan)
            {
                ereport(WARNING,
                    errmsg("[pg_lab] Ignoring global parallelization setting"),
                    errdetail("Global parallelization setting is overridden by operator hints"));
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

            hints_->contains_hint = true;
        }

        void enterJoin_order_hint(pg_lab::HintBlockParser::Join_order_hintContext *ctx) override
        {
            if (hints_->join_prefixes)
                ereport(ERROR, errmsg("Cannot combine JoinOrder hint with JoinPrefix hint"));

            auto join_order = this->ParseJoinOrder(ctx->join_order_entry());
            hints_->join_order_hint = join_order;
            hints_->contains_hint = true;

            #ifdef PGLAB_TRACE

            StringInfo joinorder_debug = makeStringInfo();
            joinorder_to_string(join_order, joinorder_debug);
            ereport(INFO, (errmsg("Creating join order hint"), errdetail("%s", joinorder_debug->data)));
            destroyStringInfo(joinorder_debug);

            #endif

        }

        void enterJoin_prefix_hint(pg_lab::HintBlockParser::Join_prefix_hintContext *ctx) override
        {
            if (hints_->join_order_hint)
                ereport(ERROR, errmsg("Cannot combine JoinPrefix hint with JoinOrder hint"));

            auto join_order = this->ParseJoinOrder(ctx->join_order_entry());
            hints_->join_prefixes = lappend(hints_->join_prefixes, join_order);
            hints_->contains_hint = true;
        }

        void enterJoin_op_hint(pg_lab::HintBlockParser::Join_op_hintContext *ctx) override
        {

            List *relnames = NIL;
            for (const auto &rel_ctx : ctx->binary_rel_id()->relation_id())
            {
                auto relname = pstrdup(rel_ctx->getText().c_str());
                relnames = lappend(relnames, relname);
            }

            for (const auto &rel_ctx : ctx->relation_id())
            {
                auto relname = pstrdup(rel_ctx->getText().c_str());
                relnames = lappend(relnames, relname);
            }


            PhysicalOperator op = OP_UNKNOWN;
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

            ParseOperatorHint(relnames, op, ctx->param_list());
            list_free_deep(relnames);
        }

        void enterScan_op_hint(pg_lab::HintBlockParser::Scan_op_hintContext *ctx) override {
            auto relname = pstrdup(ctx->relation_id()->getText().c_str());
            List *relnames = list_make1(relname);

            PhysicalOperator op = OP_UNKNOWN;
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


            ParseOperatorHint(relnames, op, ctx->param_list());
            list_free_deep(relnames);
        }

        void enterResult_hint(pg_lab::HintBlockParser::Result_hintContext *ctx) override
        {
            if (!ctx->parallel_hint())
            {
                ereport(WARNING, errmsg("[pg_lab] Ignoring empty Result hint"));
                return;
            }

            if (hints_->parallelize_entire_plan)
            {
                ereport(WARNING,
                    errmsg("[pg_lab] Found multiple parallel hints"),
                    errdetail("Postgres normally creates only one parallel subplan. This might break the query."));
            }

            hints_->parallel_workers = ParseParallelWorkers(ctx->parallel_hint());
            hints_->parallelize_entire_plan = true;
            hints_->parallel_mode = PARMODE_PARALLEL;
        }

        void enterCardinality_hint(pg_lab::HintBlockParser::Cardinality_hintContext *ctx) override
        {
            List *relnames = NIL;
            for (const auto &rel_ctx : ctx->relation_id())
            {
                auto relname = pstrdup(rel_ctx->getText().c_str());
                relnames = lappend(relnames, relname);
            }

            Cardinality cardinality = std::atof(ctx->INT()->getText().c_str());

            MakeCardHint(root_, hints_, relnames, cardinality);
            list_free_deep(relnames);
        }

        void enterGuc_hint(pg_lab::HintBlockParser::Guc_hintContext *ctx) override
        {
            auto guc_name = ctx->guc_name()->getText();
            auto guc_value = ctx->guc_value()->getText();

            auto cleanup = MakeGUCHint(hints_, guc_name.c_str(), guc_value.c_str());
            if (cleanup)
                temp_gucs_.push_back(cleanup);
        }

        void ExportGucCleanup()
        {
            InitGucCleanup(temp_gucs_.size());
            for (const auto &temp_guc : temp_gucs_)
                StoreGucCleanup(temp_guc);
        }

    private:
        PlannerInfo  *root_;
        PlannerHints *hints_;
        std::vector<TempGUC *> temp_gucs_;

        float ParseParallelWorkers(pg_lab::HintBlockParser::Parallel_hintContext *ctx)
        {
            if (!ctx->INT())
            {
                ereport(ERROR, errmsg("[pg_lab] Invalid parallel hint format: '%s'", ctx->getText().c_str()));
                return NAN;
            }

            return std::atof(ctx->INT()->getText().c_str());
        }

        void ParseOperatorHint(List *relnames, PhysicalOperator op, pg_lab::HintBlockParser::Param_listContext *ctx)
        {
            float par_workers = NAN;
            if (ctx && ctx->parallel_hint().size() > 0)
            {
                if (hints_->parallel_rels || hints_->parallelize_entire_plan)
                {
                    ereport(ERROR, (
                            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("[pg_lab] Found multiple parallel hints"),
                            errdetail("Postgres only supports one parallel subplan.")));
                }

                par_workers = ParseParallelWorkers(ctx->parallel_hint().back());
            }

            if (ctx && ctx->cost_hint().size() > 1)
            {
                auto cost_hint = ctx->cost_hint().back();
                if (cost_hint->cost().size() != 2)
                {
                    ereport(ERROR, errmsg("[pg_lab] Invalid cost hint format: '%s'", cost_hint->getText().c_str()));
                    return;
                }
                auto startup_cost = ParseCost(cost_hint->cost().front());
                auto total_cost = ParseCost(cost_hint->cost().back());
                MakeCostHint(root_, hints_, relnames, op, startup_cost, total_cost);
                return;
            }


            if (op == OP_MEMOIZE)
                MakeIntermediateOpHint(root_, hints_, relnames, false, true, par_workers);
            else if (op == OP_MATERIALIZE)
                MakeIntermediateOpHint(root_, hints_, relnames, true, false, par_workers);
            else
                MakeOperatorHint(root_, hints_, relnames, op, par_workers);
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
            Assert(ctx->join_order_entry().size() == 2);
            auto outer_child = ParseJoinOrder(ctx->join_order_entry().front());
            auto inner_child = ParseJoinOrder(ctx->join_order_entry().back());
            return MakeJoinOrderIntermediate(root_, outer_child, inner_child);
        }

        JoinOrder *ParseJoinOrderBase(pg_lab::HintBlockParser::Base_join_orderContext *ctx)
        {
            auto relname = pstrdup(ctx->relation_id()->getText().c_str());
            auto join_order = MakeJoinOrderBase(root_, relname);
            pfree(relname);
            return join_order;
        }

};

extern "C" void
parse_hint_block(PlannerInfo *root, PlannerHints *hints)
{
    std::string query_buffer = std::string(hints->raw_query);
    auto hb_start = query_buffer.find("/*=pg_lab=");
    auto hb_end   = query_buffer.find("*/");
    if (hb_start == std::string::npos || hb_end == std::string::npos)
    {
        hints->contains_hint = false;
        return;
    }

    auto hint_string = query_buffer.substr(hb_start, hb_end - hb_start + 2);
    hints->raw_hint = (char *) pstrdup(hint_string.c_str());

    antlr4::ANTLRInputStream parser_input(hint_string);
    pg_lab::HintBlockLexer lexer(&parser_input);
    antlr4::CommonTokenStream tokens(&lexer);
    pg_lab::HintBlockParser parser(&tokens);

    antlr4::tree::ParseTree *tree = parser.hint_block();
    HintBlockListener listener(root, hints);
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(&listener, tree);
    listener.ExportGucCleanup();

    if (hints->mode == HINTMODE_FULL && hints->parallel_mode == PARMODE_DEFAULT)
        hints->parallel_mode = PARMODE_SEQUENTIAL;
}
