
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

inline static bool RTIndexIsValid(Index rt_index);
static Index resolve_rt_index(PlannerInfo *root, const char* relname);


class HintBlockListener : public pg_lab::HintBlockBaseListener
{
    public:
        explicit HintBlockListener(PlannerInfo *root, PlannerHints *hints) : root_(root), hints_(hints) {}

        void enterJoin_order_hint(pg_lab::HintBlockParser::Join_order_hintContext *ctx) override {
            auto join_order = this->ParseJoinOrderIntermediate(ctx->intermediate_join_order());
            hints_->join_order_hint = join_order;
        }

        void enterJoin_op_hint(pg_lab::HintBlockParser::Join_op_hintContext *ctx) override {
            Relids relations = EMPTY_BITMAP;
            for (const auto &relname : ctx->binary_rel_id()->relation_id()) {
                Index rt_index = resolve_rt_index(root_, relname->getText().c_str());
                relations = bms_add_member(relations, rt_index);
            }

            for (const auto &relname : ctx->relation_id()) {
                Index rt_index = resolve_rt_index(root_, relname->getText().c_str());
                relations = bms_add_member(relations, rt_index);
            }

            OperatorHashEntry *operator_hint = (OperatorHashEntry*) hash_search(hints_->operator_hints,
                                                                                &relations, HASH_ENTER, NULL);

            if (ctx->NESTLOOP()) {
                operator_hint->op = NESTLOOP;
            } else if (ctx->HASHJOIN()) {
                operator_hint->op = HASHJOIN;
            } else if (ctx->MERGEJOIN()) {
                operator_hint->op = MERGEJOIN;
            } else {
                /* XXX: appropriate error handling */
            }

            if (ctx->cost_hint()) {
                ExtractOperatorCost(operator_hint, ctx->cost_hint());
            } else {
                operator_hint->hint_type = FORCED_OP;
            }
        }

        void enterScan_op_hint(pg_lab::HintBlockParser::Scan_op_hintContext *ctx) override {
            auto relname = ctx->relation_id()->getText().c_str();
            Index rt_index = resolve_rt_index(root_, relname);
            Bitmapset *operator_key = bms_make_singleton(rt_index);
            OperatorHashEntry* operator_hint = (OperatorHashEntry*) hash_search(hints_->operator_hints,
                                                                                &operator_key, HASH_ENTER, NULL);

            if (ctx->SEQSCAN()) {
                operator_hint->op = SEQSCAN;
            } else if (ctx->IDXSCAN()) {
                operator_hint->op = IDXSCAN;
            } else {
                /* XXX: appropriate error handling */
            }

            if (ctx->cost_hint()) {
                ExtractOperatorCost(operator_hint, ctx->cost_hint());
            } else {
                operator_hint->hint_type = FORCED_OP;
            }
        }

        void enterCardinality_hint(pg_lab::HintBlockParser::Cardinality_hintContext *ctx) override {
            Relids relations = EMPTY_BITMAP;
            for (const auto &relname : ctx->relation_id()) {
                Index rt_index = resolve_rt_index(root_, relname->getText().c_str());
                relations = bms_add_member(relations, rt_index);
            }
            auto cardinality = std::atoi(ctx->INT()->getText().c_str());

            CardinalityHashEntry *cardinality_hint = (CardinalityHashEntry*) hash_search(hints_->cardinality_hints,
                                                                                         &relations, HASH_ENTER, NULL);
            cardinality_hint->card = cardinality;
        }

    private:
        PlannerInfo *root_;
        PlannerHints *hints_;

        void ExtractOperatorCost(OperatorHashEntry *operator_hint, pg_lab::HintBlockParser::Cost_hintContext *ctx) {
            auto startup_cost = std::atof(ctx->FLOAT().at(0)->getText().c_str());
            auto total_cost = std::atof(ctx->FLOAT().at(1)->getText().c_str());

            operator_hint->hint_type = COST_OP;
            operator_hint->startup_cost = startup_cost;
            operator_hint->total_cost = total_cost;
        }

        JoinOrder *ParseJoinOrder(pg_lab::HintBlockParser::Join_order_entryContext *ctx) {
            auto base_join_order = ctx->base_join_order();
            if (base_join_order)
                return ParseJoinOrderBase(base_join_order);
            else
                return ParseJoinOrderIntermediate(ctx->intermediate_join_order());
        }

        JoinOrder *ParseJoinOrderIntermediate(pg_lab::HintBlockParser::Intermediate_join_orderContext *ctx) {
            auto entries = ctx->join_order_entry();
            /* XXX: robustness (array accesses + don't merge nodes with overlapping relations) */
            auto outer_child = ParseJoinOrder(entries.at(0));
            auto inner_child = ParseJoinOrder(entries.at(1));
            Relids relation_bms = bms_union(outer_child->relids, inner_child->relids);

            JoinOrder *join_order = (JoinOrder*) palloc0(sizeof(JoinOrder));
            join_order->node_type = JOIN_REL;
            join_order->relids = relation_bms;
            join_order->outer_child = outer_child;
            join_order->inner_child = inner_child;

            outer_child->parent_node = join_order;
            inner_child->parent_node = join_order;

            auto current_level = std::max(outer_child->level, inner_child->level);
            join_order->level = current_level + 1;

            return join_order;
        }

        JoinOrder *ParseJoinOrderBase(pg_lab::HintBlockParser::Base_join_orderContext *ctx) {
            char *relname = pstrdup(ctx->relation_id()->getText().c_str());
            Index rt_index = resolve_rt_index(root_, relname);
            Bitmapset *relation_bms = bms_make_singleton(rt_index);

            JoinOrder *join_order = (JoinOrder*) palloc0(sizeof(JoinOrder));
            join_order->node_type = BASE_REL;
            join_order->relids = relation_bms;
            join_order->rt_index = rt_index;
            join_order->base_identifier = relname;
            join_order->level = 0;

            return join_order;
        }
};

inline static bool
RTIndexIsValid(Index rt_index) {
    return rt_index >= 0;
}

static Index resolve_rt_index(PlannerInfo *root, const char *relname_or_alias)
{
    RelOptInfo *rel;
    RangeTblEntry *rte;

    for (int i = 1; i < root->simple_rel_array_size; ++i)
    {
        rel = root->simple_rel_array[i];
        rte = root->simple_rte_array[i];
        if (rel == NULL || rte == NULL)
            continue;

        if (rte->eref->aliasname && strcmp(rte->eref->aliasname, relname_or_alias) == 0)
            return rel->relid;
    }

    return -1;
}


void init_hints(PlannerInfo *root, PlannerHints *hints) {
    hints->raw_query = current_query_string;
    std::string *query_buffer = new std::string(hints->raw_query);

    /* TODO: invoke hint parser, extract and resolve aliases to OIDs */
    auto hint_block_start = query_buffer->find("/*=pg_lab=");
    auto hint_block_end = query_buffer->find("*/");
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
    op_hints_hctl.entrysize = sizeof(OperatorHashEntry);
    op_hints_hctl.hcxt = CurrentMemoryContext;
    op_hints_hctl.hash = bitmap_hash;
    op_hints_hctl.match = bitmap_match;
    hints->operator_hints = hash_create("OperatorHintHashes",
                                        16L,
                                        &op_hints_hctl,
                                        HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    HASHCTL card_hints_hctl;
    card_hints_hctl.keysize = sizeof(Relids);
    card_hints_hctl.entrysize = sizeof(CardinalityHashEntry);
    card_hints_hctl.hcxt = CurrentMemoryContext;
    card_hints_hctl.hash = bitmap_hash;
    card_hints_hctl.match = bitmap_match;
    hints->cardinality_hints = hash_create("CardinalityHintHashes",
                                           32L,
                                           &card_hints_hctl,
                                           HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    /*
     * C++ extensions for Postgres should only put POD-objects onto the stack. This enables correct unrolling in case of
     * errors, thereby keeping the current connection alive. Therefore, we need to allocate all non-POD objects on the heap.
     * Even worse, we are forced to use explicit new/delete syntax here, because smart pointers once again are no PODs.
     *
     * Notice that this code will leak in case of errors because we cannot clean up the allocated memory correctly.
     *
     * XXX: Maybe we can figure something out to prevent leakage.
     */
    antlr4::ANTLRInputStream *parser_input = new antlr4::ANTLRInputStream(hint_string);
    pg_lab::HintBlockLexer *lexer = new pg_lab::HintBlockLexer(parser_input);
    antlr4::CommonTokenStream *tokens = new antlr4::CommonTokenStream(lexer);
    pg_lab::HintBlockParser *parser = new pg_lab::HintBlockParser(tokens);

#ifdef __DEBUG__
    StringInfo tokens_msg = makeStringInfo();
    for (auto token : tokens->getTokens())
    {
        appendStringInfo(tokens_msg, "%s ", token->toString().c_str());
    }
    ereport(INFO, errmsg("pg_lab"), errdetail("Tokens: %s", tokens_msg->data));
    destroyStringInfo(tokens_msg);
#endif

    antlr4::tree::ParseTree *tree = parser->hint_block();
    HintBlockListener *listener = new HintBlockListener(root, hints);
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(listener, tree);

    delete listener;
    delete parser;
    delete tokens;
    delete lexer;
    delete parser_input;
    delete query_buffer;
}


void free_hints(PlannerHints *hints) {
    if (!hints)
        return;

    if (hints->join_order_hint)
        free_join_order(hints->join_order_hint);

    if (hints->operator_hints)
        hash_destroy(hints->operator_hints);

    if (hints->cardinality_hints)
        hash_destroy(hints->cardinality_hints);
}
