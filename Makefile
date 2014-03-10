clean:
	rm -rf build proto/*.[ch]

.DEFAULT_GOAL = all

proto/%.c: proto/%.proto
	protoc-c --proto_path proto --c_out proto proto/*.proto

all: proto
	mkdir -p build
	$(CC) -o build/fuse-dfs-proto proto/*.c src/*.c -lprotobuf-c -lprotobuf-c-rpc -I.
