
#ifndef HINT_PARSER_H
#define HINT_PARSER_H

extern "C" {
#include "nodes/pathnodes.h"
#include "hints.h"
}

extern void init_hints(PlannerInfo *root, PlannerHints *hints);
extern void free_hints(PlannerHints *hints);

#endif // HINT_PARSER_H
