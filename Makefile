clean:
	rm -rf build proto/*.[ch]

.PHONY : proto

proto: proto/*.proto
	protoc-c --proto_path proto --c_out proto proto/*.proto


build/fuse-dfs-proto:
	$(CC) -o build/fuse-dfs-proto proto/*.c src/*.c -lprotoc
