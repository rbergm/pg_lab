#ifndef PGLAB_HINTPARSE_SUPPORT_H
#define PGLAB_HINTPARSE_SUPPORT_H

#include "postgres.h"
typedef struct JoinOrder JoinOrder;
#include "hints.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *yyscan_t;

typedef struct HintParseState
{
    PlannerInfo *root;
    PlannerHints *hints;
    List *guc_cleanup;
    bool had_syntax_error;
} HintParseState;

typedef struct HintParamList
{
    bool forced;
    bool has_cost;
    Cost startup_cost;
    Cost total_cost;
    bool has_parallel;
    float parallel_workers;
} HintParamList;

extern int pglab_yyparse(HintParseState *state, yyscan_t yyscanner);
extern void pglab_scanner_init(const char *input, yyscan_t *scanner_out);
extern void pglab_scanner_finish(yyscan_t scanner);
extern void pglab_yyerror(HintParseState *state, yyscan_t yyscanner, const char *message);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* PGLAB_HINTPARSE_SUPPORT_H */
