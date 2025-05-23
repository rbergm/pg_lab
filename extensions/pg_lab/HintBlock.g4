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
    ;

plan_mode_setting
    : PLANMODE EQ (FULL | ANCHORED)
    ;

join_order_hint
    : JOINORDER LPAREN intermediate_join_order RPAREN
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
    ;

join_op_hint
    : (NESTLOOP | HASHJOIN | MERGEJOIN | MEMOIZE | MATERIALIZE) LPAREN binary_rel_id relation_id* (forced_hint? | cost_hint) RPAREN
    ;

scan_op_hint
    : (SEQSCAN | IDXSCAN | BITMAPSCAN | MEMOIZE | MATERIALIZE) LPAREN relation_id (forced_hint? | cost_hint) RPAREN
    ;

cardinality_hint
    : CARD LPAREN relation_id+ HASH INT RPAREN
    ;

cost_hint
    : LPAREN COST STARTUP EQ cost TOTAL EQ cost RPAREN
    ;

forced_hint
    : LPAREN FORCED RPAREN
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
HBLOCK_END   : '*/'  ;
LPAREN       : '('   ;
RPAREN       : ')'   ;
LBRACE       : '{'   ;
RBRACE       : '}'   ;
LBRACKET     : '['   ;
RBRACKET     : ']'   ;
HASH         : '#'   ;
EQ           : '='   ;
DOT          : '.'   ;
SEM          : ';'   ;

CONFIG      : 'Config'      ;
PLANMODE    : 'plan_mode'   ;
JOINORDER   : 'JoinOrder'   ;
NESTLOOP    : 'NestLoop'    ;
MERGEJOIN   : 'MergeJoin'   ;
HASHJOIN    : 'HashJoin'    ;
SEQSCAN     : 'SeqScan'     ;
IDXSCAN     : 'IdxScan'     ;
BITMAPSCAN  : 'BitmapScan'  ;
MEMOIZE     : 'Memo'        ;
MATERIALIZE : 'Material'    ;
CARD        : 'Card'        ;
COST        : 'Cost'        ;
STARTUP     : 'Start'       ;
TOTAL       : 'Total'       ;
FORCED      : 'Forced'      ;
FULL        : 'full'        ;
ANCHORED    : 'anchored'    ;

REL_ID      : [a-z_][a-z_0-9]*  ;
FLOAT       : [0-9]+ DOT [0-9]+ ;
INT         : [0-9]+            ;

WS          : [ \t\r\n\u000C]+ -> skip; // skip spaces, tabs, newlines
