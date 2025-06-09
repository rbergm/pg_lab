grammar HintBlock;
options { caseInsensitive = true; }

hint_block : HBLOCK_START hints* HBLOCK_END EOF ;

hints
    : setting_hint
    | join_order_hint
    | operator_hint
    | cardinality_hint
    | cost_hint
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
    : PLANMODE EQ (FULL | ANCHORED)
    ;

parallelization_setting
    : PARMODE EQ (DEFAULT | SEQUENTIAL | PARALLEL)
    ;

join_order_hint
    : JOINORDER LPAREN join_order_entry RPAREN
    ;

join_order_entry
    : base_join_order
    | intermediate_join_order
    ;

base_join_order
    : relation_id
    ;

intermediate_join_order
    : LPAREN join_order_entry join_order_entry RPAREN
    ;

operator_hint
    : join_op_hint
    | scan_op_hint
    | result_hint
    ;

join_op_hint
    : (NESTLOOP | HASHJOIN | MERGEJOIN | MEMOIZE | MATERIALIZE) LPAREN binary_rel_id relation_id* param_list? RPAREN
    ;

scan_op_hint
    : (SEQSCAN | IDXSCAN | BITMAPSCAN | MEMOIZE | MATERIALIZE) LPAREN relation_id param_list? RPAREN
    ;

result_hint
    : RESULT LPAREN parallel_hint RPAREN
    ;

cardinality_hint
    : CARD LPAREN relation_id+ HASH INT RPAREN
    ;

param_list
    : LPAREN (forced_hint | cost_hint | parallel_hint)+ RPAREN
    ;

cost_hint
    : COST LPAREN STARTUP EQ cost TOTAL EQ cost RPAREN
    ;

parallel_hint
    : WORKERS EQ INT
    ;

forced_hint
    : FORCED
    ;

binary_rel_id
    : relation_id relation_id
    ;

relation_id
    : REL_ID
    ;

cost
    : FLOAT
    | INT
    ;

// LEXER part

HBLOCK_START : '/*=pg_lab=' ;
HBLOCK_END   : '*/'         ;
LPAREN       : '('          ;
RPAREN       : ')'          ;
LBRACE       : '{'          ;
RBRACE       : '}'          ;
LBRACKET     : '['          ;
RBRACKET     : ']'          ;
HASH         : '#'          ;
EQ           : '='          ;
DOT          : '.'          ;
SEM          : ';'          ;
DEFAULT      : 'default'    ;

// Config
CONFIG      : 'Config'      ;
PLANMODE    : 'plan_mode'   ;
FULL        : 'full'        ;
ANCHORED    : 'anchored'    ;
PARMODE     : 'exec_mode'   ;
SEQUENTIAL  : 'sequential'  ;
PARALLEL    : 'parallel'    ;


// Top-level hints
JOINORDER   : 'JoinOrder'   ;
CARD        : 'Card'        ;

// Operators
NESTLOOP    : 'NestLoop'    ;
MERGEJOIN   : 'MergeJoin'   ;
HASHJOIN    : 'HashJoin'    ;
SEQSCAN     : 'SeqScan'     ;
IDXSCAN     : 'IdxScan'     ;
BITMAPSCAN  : 'BitmapScan'  ;
MEMOIZE     : 'Memo'        ;
MATERIALIZE : 'Material'    ;
RESULT      : 'Result'      ;

// Operator parameters
COST        : 'Cost'        ;
STARTUP     : 'Start'       ;
TOTAL       : 'Total'       ;
WORKERS     : 'Workers'     ;
FORCED      : 'Forced'      ;


REL_ID      : [a-z_][a-z_0-9]*  ;
FLOAT       : [0-9]+ DOT [0-9]+ ;
INT         : [0-9]+            ;

WS          : [ \t\r\n\u000C]+ -> skip; // skip spaces, tabs, newlines
