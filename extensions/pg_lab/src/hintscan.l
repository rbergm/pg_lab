%top{
#include "postgres.h"
#include <string.h>

#include "hintparse.h"
#define YYSTYPE PGLAB_YYSTYPE
#include "hintparse_support.h"
}

%{
/* No reason to constrain the input size for hint parsing. */
#define YY_READ_BUF_SIZE 16777216

/* Avoid calling exit() on fatal scanner errors. */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
    ereport(ERROR, (errmsg_internal("%s", msg)));
}
%}

%option reentrant
%option bison-bridge
%option 8bit
%option case-insensitive
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="pglab_yy"

integer      [0-9]+
float        {integer}\.{integer}
identifier   [a-z_][a-z_0-9]*

%%

"/*=pg_lab="           return HBLOCK_START;
"*/"                   return HBLOCK_END;
"("                    return LPAREN;
")"                    return RPAREN;
"#"                    return HASH;
"="                    return EQ;
";"                    return SEM;
"'"                    return QUOTE;

"config"               return CONFIG;
"plan_mode"            return PLANMODE;
"full"                 return FULL;
"anchored"             return ANCHORED;
"exec_mode"            return PARMODE;
"default"              return DEFAULT;
"sequential"           return SEQUENTIAL;
"parallel"             return PARALLEL;
"set"                  return SET;

"joinorder"            return JOINORDER;
"joinprefix"           return JOINPREFIX;
"card"                 return CARD;

"nestloop"             return NESTLOOP;
"mergejoin"            return MERGEJOIN;
"hashjoin"             return HASHJOIN;
"seqscan"              return SEQSCAN;
"idxscan"              return IDXSCAN;
"bitmapscan"           return BITMAPSCAN;
"memo"                 return MEMOIZE;
"material"             return MATERIALIZE;
"result"               return RESULT;

"cost"                 return COST;
"start"                return STARTUP;
"total"                return TOTAL;
"workers"              return WORKERS;
"forced"               return FORCED;

{float}                { yylval->str = pstrdup(yytext); return FLOAT; }
{integer}              { yylval->str = pstrdup(yytext); return INT; }
{identifier}           { yylval->str = pstrdup(yytext); return IDENTIFIER; }

[ \t\n\r\f\v]+      /* skip whitespace */
.                      return yytext[0];

%%

void
pglab_yyerror(HintParseState *state, yyscan_t yyscanner, const char *message)
{
    struct yyguts_t *yyg = (struct yyguts_t *) yyscanner;

    state->had_syntax_error = true;

    if (*yytext == YY_END_OF_BUFFER_CHAR)
    {
        ereport(WARNING,
                (errmsg("[pg_lab] Malformed hint block"),
                 errdetail("%s at end of input", message)));
    }
    else
    {
        ereport(WARNING,
                (errmsg("[pg_lab] Malformed hint block"),
                 errdetail("%s at or near \"%s\"", message, yytext)));
    }
}

void
pglab_scanner_init(const char *input, yyscan_t *scanner_out)
{
    if (yylex_init(scanner_out) != 0)
        elog(ERROR, "yylex_init() failed: %m");

    yy_scan_bytes(input, strlen(input), *scanner_out);
}

void
pglab_scanner_finish(yyscan_t scanner)
{
    yylex_destroy(scanner);
}

void *
yyalloc(yy_size_t size, yyscan_t yyscanner)
{
    return palloc(size);
}

void *
yyrealloc(void *ptr, yy_size_t size, yyscan_t yyscanner)
{
    if (ptr)
        return repalloc(ptr, size);

    return palloc(size);
}

void
yyfree(void *ptr, yyscan_t yyscanner)
{
    if (ptr)
        pfree(ptr);
}
