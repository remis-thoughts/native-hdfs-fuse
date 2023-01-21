clean:
	rm -rf build proto/*.[ch]

UNCRUSTIFY = uncrustify -c uncrustify.cfg -l C --replace --no-backup
.DEFAULT_GOAL = all
.PHONY : proto

build:
	mkdir build

build/proto: proto/*.proto build
	protoc-c --proto_path proto --c_out proto proto/*.proto
	$(UNCRUSTIFY) proto/*.[ch]
	touch build/proto

CCFLAGS := $(shell pkg-config --cflags --libs fuse libprotobuf-c) -Werror -Wall -Wextra -I. -std=gnu99 -Wno-unused

build/native-hdfs-fuse: src/*.c src/*.h build/proto build
	mkdir -p build
	$(UNCRUSTIFY) proto/*.[ch] src/*.[ch]
	$(CC) -o build/native-hdfs-fuse proto/*.c src/*.c $(CCFLAGS)

all: CC += -DNDEBUG
all: build/native-hdfs-fuse

debug: CC += -DDEBUG -g3
debug: build/native-hdfs-fuse

install:
	install build/native-hdfs-fuse /usr/bin
