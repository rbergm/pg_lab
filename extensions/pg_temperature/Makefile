# extensions/pg_temperature/Makefile

MODULES = pg_temperature
EXTENSION = $(MODULES)
DATA = $(MODULES)--0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
