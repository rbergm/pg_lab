MODULE_big = pg_lab
OBJS = \
	$(WIN32RES) \
	src/pg_lab.o \
	src/hints.o \
	src/hint_parser.o \
	src/hintparse.o \
	src/hintscan.o

PGFILEDESC = "pg_lab - query planner hinting"

PG_CPPFLAGS += -I$(srcdir)/include -I$(srcdir)/src
SHLIB_LINK += $(filter -lm, $(LIBS))

EXTRA_CLEAN = src/hintparse.h src/hintparse.c src/hintscan.c

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

src/hintparse.h: src/hintparse.c
	touch $@

src/hintparse.c: BISONFLAGS += -d

src/hintparse.o src/hintscan.o src/hint_parser.o: src/hintparse.h
