uuid.dylib:
	gcc -g -fPIC -dynamiclib uuid.c -o uuid.dylib
hlc.dylib:
	gcc -g -fPIC -dynamiclib hlc.c -o hlc.dylib
crdt.dylib:
	gcc -g -fPIC -dynamiclib crdt.c -o crdt.dylib
vendor/sqlite3.c:
	mkdir -p vendor
	curl -o sqlite-amalgamation.zip https://www.sqlite.org/2024/sqlite-amalgamation-3450300.zip
	unzip sqlite-amalgamation.zip
	mv sqlite-amalgamation-3450300/* vendor/
	rmdir sqlite-amalgamation-3450300
	rm sqlite-amalgamation.zip
sqlite3: vendor/sqlite3.c
	gcc vendor/shell.c vendor/sqlite3.c -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -lpthread -ldl -lm -o sqlite3
clean:
	rm -f uuid.dylib
	rm -f hlc.dylib
	rm -f crdt.dylib
	rm -rf crdt.dylib.dSYM
	rm -rf hlc.dylib.dSYM
	rm -rf uuid.dylib.dSYM
all: sqlite3
	make clean
	make uuid.dylib
	make hlc.dylib
	make crdt.dylib
