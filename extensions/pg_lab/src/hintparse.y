%{
#include <math.h>
#include <stdlib.h>

#include "postgres.h"

#include "hintparse_support.h"

#define YYMALLOC palloc
#define YYFREE   pfree

static HintParamList *MakeEmptyParams(void);
static HintParamList *MergeParams(HintParamList *lhs, HintParamList *rhs);
static Cost ParseCostLiteral(const char *literal);
static float ParseWorkersLiteral(const char *literal);
static void ApplyOperatorHint(HintParseState *state, List *relnames, PhysicalOperator op, HintParamList *params);
union PGLAB_YYSTYPE;
int pglab_yylex(union PGLAB_YYSTYPE *yylval_param, yyscan_t yyscanner);

%}

%define api.prefix {pglab_yy}
%define api.pure full
%define parse.error verbose

%code requires {
#include "hintparse_support.h"
}

%parse-param {HintParseState *state}
%parse-param {yyscan_t yyscanner}
%lex-param   {yyscan_t yyscanner}

%union {
    char *str;
    List *list;
    JoinOrder *join_order;
    PhysicalOperator op;
    HintParamList *params;
}

%token HBLOCK_START HBLOCK_END
%token LPAREN RPAREN HASH EQ SEM QUOTE
%token DEFAULT

%token CONFIG PLANMODE FULL ANCHORED PARMODE SEQUENTIAL PARALLEL
%token SET

%token JOINORDER JOINPREFIX CARD

%token NESTLOOP MERGEJOIN HASHJOIN SEQSCAN IDXSCAN BITMAPSCAN MEMOIZE MATERIALIZE RESULT

%token COST STARTUP TOTAL WORKERS FORCED

%token <str> IDENTIFIER FLOAT INT

%type <join_order> join_order_entry
%type <list> relid_list relid_tail
%type <params> param_list_opt param_list param_entries param_entry cost_param parallel_param
%type <str> cost_value guc_literal

%start hint_block

%%

hint_block
    : HBLOCK_START hint_items_opt HBLOCK_END
    ;

hint_items_opt
    :
    | hint_items
    ;

hint_items
    : hint_items hint_item
    | hint_item
    ;

hint_item
    : setting_hint
    | join_order_hint
    | join_prefix_hint
    | join_op_hint
    | scan_op_hint
    | result_hint
    | cardinality_hint
    | standalone_cost_hint
    | guc_hint
    | unknown_hint
    | error
      {
          state->had_syntax_error = true;
          ereport(WARNING,
                  (errmsg("[pg_lab] Ignoring malformed hint")));
          yyerrok;
      }
    ;

setting_hint
    : CONFIG LPAREN setting_list RPAREN
    ;

setting_list
    : setting_list SEM setting
    | setting
    ;

setting
    : plan_mode_setting
    | parallelization_setting
    ;

plan_mode_setting
    : PLANMODE EQ FULL
      {
          state->hints->mode = HINTMODE_FULL;
          state->hints->contains_hint = true;
      }
    | PLANMODE EQ ANCHORED
      {
          state->hints->mode = HINTMODE_ANCHORED;
          state->hints->contains_hint = true;
      }
    ;

parallelization_setting
    : PARMODE EQ PARALLEL
      {
          if (state->hints->parallel_rels || state->hints->parallelize_entire_plan)
          {
              ereport(WARNING,
                      (errmsg("[pg_lab] Ignoring global parallelization setting"),
                       errdetail("Global parallelization setting is overridden by operator hints")));
          }
          else
          {
              state->hints->parallel_mode = PARMODE_PARALLEL;
              state->hints->contains_hint = true;
          }
      }
    | PARMODE EQ SEQUENTIAL
      {
          if (state->hints->parallel_rels || state->hints->parallelize_entire_plan)
          {
              ereport(WARNING,
                      (errmsg("[pg_lab] Ignoring global parallelization setting"),
                       errdetail("Global parallelization setting is overridden by operator hints")));
          }
          else
          {
              state->hints->parallel_mode = PARMODE_SEQUENTIAL;
              state->hints->contains_hint = true;
          }
      }
    | PARMODE EQ DEFAULT
      {
          if (state->hints->parallel_rels || state->hints->parallelize_entire_plan)
          {
              ereport(WARNING,
                      (errmsg("[pg_lab] Ignoring global parallelization setting"),
                       errdetail("Global parallelization setting is overridden by operator hints")));
          }
          else
          {
              state->hints->parallel_mode = PARMODE_DEFAULT;
              state->hints->contains_hint = true;
          }
      }
    ;

join_order_hint
    : JOINORDER LPAREN join_order_entry RPAREN
      {
          if (state->hints->join_prefixes)
              ereport(ERROR, (errmsg("Cannot combine JoinOrder hint with JoinPrefix hint")));

          state->hints->join_order_hint = $3;
          state->hints->contains_hint = true;

#ifdef PGLAB_TRACE
          {
              StringInfo joinorder_debug = makeStringInfo();
              joinorder_to_string($3, joinorder_debug);
              ereport(INFO,
                      (errmsg("Creating join order hint"),
                       errdetail("%s", joinorder_debug->data)));
              destroyStringInfo(joinorder_debug);
          }
#endif
      }
    ;

join_prefix_hint
    : JOINPREFIX LPAREN join_order_entry RPAREN
      {
          if (state->hints->join_order_hint)
              ereport(ERROR, (errmsg("Cannot combine JoinPrefix hint with JoinOrder hint")));

          state->hints->join_prefixes = lappend(state->hints->join_prefixes, $3);
          state->hints->contains_hint = true;
      }
    ;

join_order_entry
    : IDENTIFIER
      {
          $$ = MakeJoinOrderBase(state->root, $1);
          pfree($1);
      }
    | LPAREN join_order_entry join_order_entry RPAREN
      {
          $$ = MakeJoinOrderIntermediate(state->root, $2, $3);
      }
    ;

join_op_hint
    : NESTLOOP LPAREN IDENTIFIER IDENTIFIER relid_tail param_list_opt RPAREN
      {
          List *relnames = list_make2($3, $4);
          relnames = list_concat(relnames, $5);

          ApplyOperatorHint(state, relnames, OP_NESTLOOP, $6);

          if ($6)
              pfree($6);

          list_free_deep(relnames);
      }
    | HASHJOIN LPAREN IDENTIFIER IDENTIFIER relid_tail param_list_opt RPAREN
      {
          List *relnames = list_make2($3, $4);
          relnames = list_concat(relnames, $5);

          ApplyOperatorHint(state, relnames, OP_HASHJOIN, $6);

          if ($6)
              pfree($6);

          list_free_deep(relnames);
      }
    | MERGEJOIN LPAREN IDENTIFIER IDENTIFIER relid_tail param_list_opt RPAREN
      {
          List *relnames = list_make2($3, $4);
          relnames = list_concat(relnames, $5);

          ApplyOperatorHint(state, relnames, OP_MERGEJOIN, $6);

          if ($6)
              pfree($6);

          list_free_deep(relnames);
      }
    | MEMOIZE LPAREN IDENTIFIER IDENTIFIER relid_tail param_list_opt RPAREN
      {
          List *relnames = list_make2($3, $4);
          relnames = list_concat(relnames, $5);

          ApplyOperatorHint(state, relnames, OP_MEMOIZE, $6);

          if ($6)
              pfree($6);

          list_free_deep(relnames);
      }
    | MATERIALIZE LPAREN IDENTIFIER IDENTIFIER relid_tail param_list_opt RPAREN
      {
          List *relnames = list_make2($3, $4);
          relnames = list_concat(relnames, $5);

          ApplyOperatorHint(state, relnames, OP_MATERIALIZE, $6);

          if ($6)
              pfree($6);

          list_free_deep(relnames);
      }
    ;

scan_op_hint
    : SEQSCAN LPAREN IDENTIFIER param_list_opt RPAREN
      {
          List *relnames = list_make1($3);

          ApplyOperatorHint(state, relnames, OP_SEQSCAN, $4);

          if ($4)
              pfree($4);

          list_free_deep(relnames);
      }
    | IDXSCAN LPAREN IDENTIFIER param_list_opt RPAREN
      {
          List *relnames = list_make1($3);

          ApplyOperatorHint(state, relnames, OP_IDXSCAN, $4);

          if ($4)
              pfree($4);

          list_free_deep(relnames);
      }
    | BITMAPSCAN LPAREN IDENTIFIER param_list_opt RPAREN
      {
          List *relnames = list_make1($3);

          ApplyOperatorHint(state, relnames, OP_BITMAPSCAN, $4);

          if ($4)
              pfree($4);

          list_free_deep(relnames);
      }
    | MEMOIZE LPAREN IDENTIFIER param_list_opt RPAREN
      {
          List *relnames = list_make1($3);

          ApplyOperatorHint(state, relnames, OP_MEMOIZE, $4);

          if ($4)
              pfree($4);

          list_free_deep(relnames);
      }
    | MATERIALIZE LPAREN IDENTIFIER param_list_opt RPAREN
      {
          List *relnames = list_make1($3);

          ApplyOperatorHint(state, relnames, OP_MATERIALIZE, $4);

          if ($4)
              pfree($4);

          list_free_deep(relnames);
      }
    ;

relid_tail
    :
      {
          $$ = NIL;
      }
    | relid_tail IDENTIFIER
      {
          $$ = lappend($1, $2);
      }
    ;

result_hint
    : RESULT LPAREN parallel_param RPAREN
      {
          if (!$3 || !$3->has_parallel)
          {
              ereport(WARNING, (errmsg("[pg_lab] Ignoring empty Result hint")));
          }
          else
          {
              if (state->hints->parallelize_entire_plan)
              {
                  ereport(WARNING,
                          (errmsg("[pg_lab] Found multiple parallel hints"),
                           errdetail("Postgres normally creates only one parallel subplan. This might break the query.")));
              }

              state->hints->parallel_workers = $3->parallel_workers;
              state->hints->parallelize_entire_plan = true;
              state->hints->parallel_mode = PARMODE_PARALLEL;
              state->hints->contains_hint = true;
          }

          if ($3)
              pfree($3);
      }
    ;

cardinality_hint
    : CARD LPAREN relid_list HASH INT RPAREN
      {
          Cardinality cardinality = strtod($5, NULL);
          MakeCardHint(state->root, state->hints, $3, cardinality);
          list_free_deep($3);
          pfree($5);
      }
    ;

relid_list
    : IDENTIFIER
      {
          $$ = list_make1($1);
      }
    | relid_list IDENTIFIER
      {
          $$ = lappend($1, $2);
      }
    ;

standalone_cost_hint
    : COST LPAREN STARTUP EQ cost_value TOTAL EQ cost_value RPAREN
      {
          state->hints->contains_hint = true;
          ereport(WARNING,
                  (errmsg("[pg_lab] Ignoring standalone Cost hint"),
                   errdetail("Cost hints must be attached to an operator hint.")));
          pfree($5);
          pfree($8);
      }
    ;

param_list_opt
    :
      {
          $$ = NULL;
      }
    | param_list
      {
          $$ = $1;
      }
    ;

param_list
    : LPAREN param_entries RPAREN
      {
          $$ = $2;
      }
    ;

param_entries
    : param_entries param_entry
      {
          $$ = MergeParams($1, $2);
      }
    | param_entry
      {
          $$ = $1;
      }
    ;

param_entry
    : FORCED
      {
          $$ = MakeEmptyParams();
          $$->forced = true;
      }
    | cost_param
      {
          $$ = $1;
      }
    | parallel_param
      {
          $$ = $1;
      }
    ;

cost_param
    : COST LPAREN STARTUP EQ cost_value TOTAL EQ cost_value RPAREN
      {
          $$ = MakeEmptyParams();
          $$->has_cost = true;
          $$->startup_cost = ParseCostLiteral($5);
          $$->total_cost = ParseCostLiteral($8);
          pfree($5);
          pfree($8);
      }
    ;

cost_value
    : INT
      {
          $$ = $1;
      }
    | FLOAT
      {
          $$ = $1;
      }
    ;

parallel_param
    : WORKERS EQ INT
      {
          $$ = MakeEmptyParams();
          $$->has_parallel = true;
          $$->parallel_workers = ParseWorkersLiteral($3);
          pfree($3);
      }
    ;

guc_hint
    : SET LPAREN IDENTIFIER EQ QUOTE guc_literal QUOTE RPAREN
      {
          TempGUC *cleanup = MakeGUCHint(state->hints, $3, $6);
          if (cleanup)
              state->guc_cleanup = lappend(state->guc_cleanup, cleanup);

          state->hints->contains_hint = true;
          pfree($3);
          pfree($6);
      }
    ;

guc_literal
    : IDENTIFIER
      {
          $$ = $1;
      }
    | FLOAT
      {
          $$ = $1;
      }
    | INT
      {
          $$ = $1;
      }
    | CONFIG
      {
          $$ = pstrdup("config");
      }
    | PLANMODE
      {
          $$ = pstrdup("plan_mode");
      }
    | FULL
      {
          $$ = pstrdup("full");
      }
    | ANCHORED
      {
          $$ = pstrdup("anchored");
      }
    | PARMODE
      {
          $$ = pstrdup("exec_mode");
      }
    | DEFAULT
      {
          $$ = pstrdup("default");
      }
    | SEQUENTIAL
      {
          $$ = pstrdup("sequential");
      }
    | PARALLEL
      {
          $$ = pstrdup("parallel");
      }
    | SET
      {
          $$ = pstrdup("set");
      }
    | JOINORDER
      {
          $$ = pstrdup("joinorder");
      }
    | JOINPREFIX
      {
          $$ = pstrdup("joinprefix");
      }
    | CARD
      {
          $$ = pstrdup("card");
      }
    | NESTLOOP
      {
          $$ = pstrdup("nestloop");
      }
    | HASHJOIN
      {
          $$ = pstrdup("hashjoin");
      }
    | MERGEJOIN
      {
          $$ = pstrdup("mergejoin");
      }
    | SEQSCAN
      {
          $$ = pstrdup("seqscan");
      }
    | IDXSCAN
      {
          $$ = pstrdup("idxscan");
      }
    | BITMAPSCAN
      {
          $$ = pstrdup("bitmapscan");
      }
    | MEMOIZE
      {
          $$ = pstrdup("memo");
      }
    | MATERIALIZE
      {
          $$ = pstrdup("material");
      }
    | RESULT
      {
          $$ = pstrdup("result");
      }
    | COST
      {
          $$ = pstrdup("cost");
      }
    | STARTUP
      {
          $$ = pstrdup("start");
      }
    | TOTAL
      {
          $$ = pstrdup("total");
      }
    | WORKERS
      {
          $$ = pstrdup("workers");
      }
    | FORCED
      {
          $$ = pstrdup("forced");
      }
    ;

unknown_hint
    : IDENTIFIER LPAREN unknown_hint_payload RPAREN
      {
          ereport(WARNING,
                  (errmsg("[pg_lab] Unknown hint \"%s\" ignored", $1)));
          pfree($1);
      }
    ;

unknown_hint_payload
    :
    | unknown_hint_payload unknown_hint_atom
    | unknown_hint_payload LPAREN unknown_hint_payload RPAREN
    ;

unknown_hint_atom
    : IDENTIFIER
      {
          pfree($1);
      }
    | FLOAT
      {
          pfree($1);
      }
    | INT
      {
          pfree($1);
      }
    | CONFIG
    | PLANMODE
    | FULL
    | ANCHORED
    | PARMODE
    | DEFAULT
    | SEQUENTIAL
    | PARALLEL
    | SET
    | JOINORDER
    | JOINPREFIX
    | CARD
    | NESTLOOP
    | HASHJOIN
    | MERGEJOIN
    | SEQSCAN
    | IDXSCAN
    | BITMAPSCAN
    | MEMOIZE
    | MATERIALIZE
    | RESULT
    | COST
    | STARTUP
    | TOTAL
    | WORKERS
    | FORCED
    | HASH
    | EQ
    | SEM
    | QUOTE
    ;

%%

static HintParamList *
MakeEmptyParams(void)
{
    HintParamList *params = (HintParamList *) palloc0(sizeof(HintParamList));

    params->startup_cost = NAN;
    params->total_cost = NAN;
    params->parallel_workers = NAN;

    return params;
}

static HintParamList *
MergeParams(HintParamList *lhs, HintParamList *rhs)
{
    if (!lhs)
        return rhs;
    if (!rhs)
        return lhs;

    lhs->forced = lhs->forced || rhs->forced;

    if (rhs->has_cost)
    {
        lhs->has_cost = true;
        lhs->startup_cost = rhs->startup_cost;
        lhs->total_cost = rhs->total_cost;
    }

    if (rhs->has_parallel)
    {
        lhs->has_parallel = true;
        lhs->parallel_workers = rhs->parallel_workers;
    }

    pfree(rhs);
    return lhs;
}

static Cost
ParseCostLiteral(const char *literal)
{
    return strtod(literal, NULL);
}

static float
ParseWorkersLiteral(const char *literal)
{
    return strtof(literal, NULL);
}

static void
ApplyOperatorHint(HintParseState *state, List *relnames, PhysicalOperator op, HintParamList *params)
{
    float par_workers = NAN;

    if (params && params->has_parallel)
    {
        if (state->hints->parallel_rels || state->hints->parallelize_entire_plan)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("[pg_lab] Found multiple parallel hints"),
                     errdetail("Postgres only supports one parallel subplan.")));
        }

        par_workers = params->parallel_workers;
    }

    if (params && params->has_cost)
    {
        MakeCostHint(state->root, state->hints, relnames, op, params->startup_cost, params->total_cost);
        return;
    }

    if (op == OP_MEMOIZE)
        MakeIntermediateOpHint(state->root, state->hints, relnames, false, true, par_workers);
    else if (op == OP_MATERIALIZE)
        MakeIntermediateOpHint(state->root, state->hints, relnames, true, false, par_workers);
    else
        MakeOperatorHint(state->root, state->hints, relnames, op, par_workers);
}
