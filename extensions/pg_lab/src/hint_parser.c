#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>

#include "postgres.h"

#include "hintparse.h"
#include "hintparse_support.h"

void
parse_hint_block(PlannerInfo *root, PlannerHints *hints)
{
    const char *query_buffer = hints->raw_query;
    const char *hb_start;
    const char *hb_end;
    Size hint_len;
    char *hint_string;
    HintParseState parse_state;
    yyscan_t scanner;
    ListCell *lc;

    hb_start = strstr(query_buffer, "/*=pg_lab=");
    if (hb_start == NULL)
    {
        hints->contains_hint = false;
        return;
    }

    hb_end = strstr(hb_start, "*/");
    if (hb_end == NULL)
    {
        hints->contains_hint = false;
        return;
    }

    hint_len = (Size) ((hb_end - hb_start) + 2);

    hint_string = (char *) palloc(hint_len + 1);
    memcpy(hint_string, hb_start, hint_len);
    hint_string[hint_len] = '\0';

    hints->raw_hint = pstrdup(hint_string);

    parse_state.root = root;
    parse_state.hints = hints;
    parse_state.guc_cleanup = NIL;
    parse_state.had_syntax_error = false;

    pglab_scanner_init(hint_string, &scanner);
    (void) pglab_yyparse(&parse_state, scanner);
    pglab_scanner_finish(scanner);

    InitGucCleanup(list_length(parse_state.guc_cleanup));
    foreach (lc, parse_state.guc_cleanup)
        StoreGucCleanup((TempGUC *) lfirst(lc));
    list_free(parse_state.guc_cleanup);

    if (hints->mode == HINTMODE_FULL && hints->parallel_mode == PARMODE_DEFAULT)
        hints->parallel_mode = PARMODE_SEQUENTIAL;

    pfree(hint_string);
}

#ifdef __cplusplus
} /* extern "C" */
#endif
