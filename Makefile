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
	clang -arch arm64 -arch x86_64 -dynamiclib \
		-DSQLITE_ENABLE_DBSTAT_VTAB \
		-DSQLITE_ENABLE_FTS5 \
		-DSQLITE_ENABLE_RTREE \
		-DSQLITE_DQS=0 \
		-DSQLITE_DEFAULT_MEMSTATUS=0 \
		-DSQLITE_TEMP_STORE=2 \
		-DSQLITE_MAX_EXPR_DEPTH=0 \
		-DSQLITE_STRICT_SUBTYPE=1 \
		-DSQLITE_OMIT_AUTHORIZATION \
		-DSQLITE_OMIT_DECLTYPE \
		-DSQLITE_OMIT_DEPRECATED \
		-DSQLITE_OMIT_PROGRESS_CALLBACK \
		-DSQLITE_OMIT_SHARED_CACHE \
		-DSQLITE_OMIT_TCL_VARIABLE \
		-DSQLITE_OMIT_TRACE \
		-DSQLITE_USE_ALLOCA \
		-DSQLITE_UNTESTABLE \
		-DSQLITE_HAVE_ISNAN \
		-DSQLITE_HAVE_LOCALTIME_R \
		-DSQLITE_HAVE_LOCALTIME_S \
		-DSQLITE_HAVE_MALLOC_USABLE_SIZE \
		-DSQLITE_HAVE_STRCHRNUL \
		-DSQLITE_ENABLE_SESSION \
		-DSQLITE_ENABLE_PREUPDATE_HOOK \
		-lpthread -ldl -lm -o sqlite3 \
		vendor/sqlite3.c
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
