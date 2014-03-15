clean:
	rm -rf build proto/*.[ch]

UNCRUSTIFY = uncrustify -c uncrustify.cfg -l C --replace --no-backup
.DEFAULT_GOAL = all
.PHONY : proto

proto: proto/*.proto
	protoc-c --proto_path proto --c_out proto proto/*.proto
	$(UNCRUSTIFY) proto/*.[ch]

CCFLAGS := $(shell pkg-config --cflags fuse libprotobuf-c) -Werror -I. -DDEBUG
LIBS := $(shell pkg-config --libs fuse libprotobuf-c)

all:
	mkdir -p build
	$(UNCRUSTIFY) proto/*.[ch] src/*.[ch]
	$(CC) -o build/fuse-dfs-proto proto/*.c src/*.c $(CCFLAGS) $(LIBS) -g
