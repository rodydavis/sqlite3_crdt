/**
** Inspired by the HLC written in Dart:
** https://github.com/cachapa/crdt/blob/master/lib/src/hlc.dart
**
** Set of HLC functions for SQLite.
**
** The HLC (Hybrid Logical Clock) is a logical clock that combines the best of
** both Lamport clocks and vector clocks. It provides a total order of events
** and is resilient to clock drift.
**
** The HLC is represented as a string in the following format:
**
**     2021-01-01T00:00:00-0000-0000-000000000000
**
** The first part is an ISO 8601 timestamp. The second part is a 4-digit
** hexadecimal counter. The third part is a node ID.
**
** The HLC is implemented as a SQLite extension with the following functions:
**
**     hlc_now(node_id TEXT) -> TEXT
**     hlc_node_id(hlc_text TEXT) -> TEXT
**     hlc_parse(timestamp TEXT) -> TEXT
**     hlc_increment(hlc_text TEXT) -> TEXT
**     hlc_merge(local_hlc_text TEXT, remote_hlc_text TEXT) -> TEXT
**     hlc_str(hlc_text TEXT) -> TEXT
**     hlc_compare(hlc_text1 TEXT, hlc_text2 TEXT) -> INT
*/

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <errno.h>
#include <assert.h>

#define MAX_COUNTER 0xFFFF
#define MAX_NODE_ID_LENGTH 64

// Represents a Duration in milliseconds
typedef int64_t Duration;
const Duration MAX_DRIFT = 60000; // 1 minute in milliseconds

// Represents the HLC structure
typedef struct {
    int64_t dateTime; // UTC milliseconds since epoch
    unsigned short counter;
    char nodeId[MAX_NODE_ID_LENGTH];
} Hlc;

// Helper function to get current UTC time in milliseconds since epoch
static int64_t getCurrentUtcMillis() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + (int64_t)tv.tv_usec / 1000;
}

// Helper function to convert struct tm to UTC milliseconds since epoch
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#include <stddef.h>
#endif

static int64_t tmToUtcMillis(struct tm *tm, long millis) {
    time_t t = mktime(tm);
    if (t == -1) {
        return -1; // Indicate error
    }
#ifdef _WIN32
    // For Windows, mktime uses local time, so we need to adjust for the timezone.
    // This is a simplified approach and might not be accurate for all timezones.
    SYSTEMTIME st;
    TzSpecificLocalTimeToSystemTime(NULL, tm, &st);
    FILETIME ft;
    SystemTimeToFileTime(&st, &ft);
    ULARGE_INTEGER uli;
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;
    return (int64_t)(uli.QuadPart / 10000) - 11644473600000LL + millis;
#else
    // For POSIX systems, mktime uses local time, so we need to convert to UTC.
    time_t utc_t = timegm(tm);
    if (utc_t == -1) {
        return -1;
    }
    return (int64_t)utc_t * 1000 + millis;
#endif
}

// Helper function to convert ISO 8601 string to UTC milliseconds since epoch
static int64_t iso8601ToUtcMillis(const char *iso8601) {
    struct tm tm;
    long millis = 0;
    char* dotPtr = strchr(iso8601, '.');
    if(dotPtr != NULL){
        if (strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%fZ", &tm) == NULL){
            if(strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%f%z", &tm) == NULL){
                if(strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%f", &tm) == NULL){
                    return -1;
                }
            }
        }
        char millisStr[4];
        strncpy(millisStr, dotPtr + 1, 3);
        millisStr[3] = '\0';
        millis = strtol(millisStr, NULL, 10);
    } else {
        if (strptime(iso8601, "%Y-%m-%dT%H:%M:%S", &tm) == NULL &&
            strptime(iso8601, "%Y-%m-%dT%H:%M:%S%z", &tm) == NULL) {
            return -1;
        }
    }

    // Assume UTC if no timezone information is provided
    if (iso8601[strlen(iso8601) - 1] != 'Z' && strchr(iso8601, '+') == NULL && strchr(iso8601, '-') == NULL) {
        tm.tm_isdst = 0; // Indicate that DST is not known, force UTC interpretation
    }
    return tmToUtcMillis(&tm, millis);
}

// Constructor: Hlc(DateTime dateTime, int counter, String nodeId)
static Hlc* hlc_create(int64_t dateTimeMillis, unsigned short counter, const char* nodeId) {
    if (counter > MAX_COUNTER || nodeId == NULL || strlen(nodeId) >= MAX_NODE_ID_LENGTH) {
        return NULL; // Indicate error with NULL
    }
    Hlc* hlc = (Hlc*)malloc(sizeof(Hlc));
    if (hlc == NULL) {
        return NULL;
    }
    hlc->dateTime = dateTimeMillis;
    hlc->counter = counter;
    strncpy(hlc->nodeId, nodeId, MAX_NODE_ID_LENGTH - 1);
    hlc->nodeId[MAX_NODE_ID_LENGTH - 1] = '\0';
    return hlc;
}

// Constructor: Hlc.zero(String nodeId)
static Hlc* hlc_zero(const char* nodeId) {
    struct tm epoch_tm;
    memset(&epoch_tm, 0, sizeof(struct tm));
    epoch_tm.tm_year = 70 - 1900; // Year since 1900
    epoch_tm.tm_mon = 0;          // Month (0-11)
    epoch_tm.tm_mday = 1;         // Day of the month (1-31)
    epoch_tm.tm_hour = 0;         // Hour (0-23)
    epoch_tm.tm_min = 0;          // Minute (0-59)
    epoch_tm.tm_sec = 0;          // Second (0-59)
    epoch_tm.tm_isdst = 0;        // Not in DST

    int64_t epochMillis = tmToUtcMillis(&epoch_tm, 0);
    if (epochMillis == -1) {
        return NULL; // Error converting time
    }
    return hlc_create(epochMillis, 0, nodeId);
}

// Constructor: Hlc.fromDate(DateTime dateTime, String nodeId)
static Hlc* hlc_fromDate(int64_t dateTimeMillis, const char* nodeId) {
    return hlc_create(dateTimeMillis, 0, nodeId);
}

// Constructor: Hlc.now(String nodeId)
static Hlc* hlc_now(const char* nodeId) {
    int64_t nowMillis = getCurrentUtcMillis();
    return hlc_fromDate(nowMillis, nodeId);
}

// Constructor: Hlc.parse(String timestamp)
static Hlc* hlc_parse(const char* timestamp) {
    if (timestamp == NULL) {
        return NULL;
    }

    const char* lastColon = strrchr(timestamp, ':');
    if (lastColon == NULL) {
        return NULL;
    }

    const char* counterDash = strchr(lastColon, '-');
    if (counterDash == NULL || counterDash == lastColon) {
        return NULL;
    }

    const char* nodeIdDash = strchr(counterDash + 1, '-');
    if (nodeIdDash == NULL || nodeIdDash == counterDash + 1) {
        return NULL;
    }

    size_t dateTimeLen = counterDash - timestamp;
    char* dateTimeStr = (char*)malloc(dateTimeLen + 1);
    if (dateTimeStr == NULL) {
        return NULL;
    }
    strncpy(dateTimeStr, timestamp, dateTimeLen);
    dateTimeStr[dateTimeLen] = '\0';

    // Handle milliseconds
    char* dotPtr = strchr(dateTimeStr, '.');
    if (dotPtr != NULL) {
        *dotPtr = '\0'; // Truncate dateTimeStr at the dot
    }

    char* counterStr = (char*)malloc(nodeIdDash - counterDash);
    if (counterStr == NULL) {
        free(dateTimeStr);
        return NULL;
    }
    strncpy(counterStr, counterDash + 1, nodeIdDash - counterDash - 1);
    counterStr[nodeIdDash - counterDash - 1] = '\0';

    const char* nodeId = nodeIdDash + 1;

    int64_t dateTime = iso8601ToUtcMillis(dateTimeStr);
    if (dateTime == -1) {
        free(dateTimeStr);
        free(counterStr);
        return NULL;
    }

    // Add milliseconds back if they were present
    if (dotPtr != NULL) {
        char millisStr[4];
        strncpy(millisStr, dotPtr + 1, 3);
        millisStr[3] = '\0';
        long millis = strtol(millisStr, NULL, 10);
        dateTime += millis;
    }

    unsigned long counter_ul = strtoul(counterStr, NULL, 16);
    if (counter_ul > MAX_COUNTER || errno == ERANGE) {
        free(dateTimeStr);
        free(counterStr);
        return NULL;
    }
    unsigned short counter = (unsigned short)counter_ul;

    if (strlen(nodeId) >= MAX_NODE_ID_LENGTH) {
        free(dateTimeStr);
        free(counterStr);
        return NULL;
    }

    Hlc* hlc = hlc_create(dateTime, counter, nodeId);
    free(dateTimeStr);
    free(counterStr);
    return hlc;
}

// Method: apply({DateTime? dateTime, int? counter, String? nodeId})
static Hlc* hlc_apply(const Hlc* hlc, int64_t dateTimeMillis, unsigned short counter, const char* nodeId) {
    if (hlc == NULL) {
        return NULL;
    }
    int64_t newDateTime = (dateTimeMillis != -1) ? dateTimeMillis : hlc->dateTime;
    unsigned short newCounter = (counter != (unsigned short)-1) ? counter : hlc->counter;
    const char* newNodeId = (nodeId != NULL) ? nodeId : hlc->nodeId;

    if (newCounter > MAX_COUNTER || strlen(newNodeId) >= MAX_NODE_ID_LENGTH) {
        return NULL;
    }

    Hlc* newHlc = (Hlc*)malloc(sizeof(Hlc));
    if (newHlc == NULL) {
        return NULL;
    }
    newHlc->dateTime = newDateTime;
    newHlc->counter = newCounter;
    strncpy(newHlc->nodeId, newNodeId, MAX_NODE_ID_LENGTH - 1);
    newHlc->nodeId[MAX_NODE_ID_LENGTH - 1] = '\0';
    return newHlc;
}

// Method: increment({DateTime? wallTime})
static Hlc* hlc_increment(const Hlc* hlc, int64_t wallTimeMillis) {
    if (hlc == NULL) {
        return NULL;
    }
    // int64_t currentWallTime = (wallTimeMillis != -1) ? wallTimeMillis : getCurrentUtcMillis();
    // int64_t dateTimeNew = (currentWallTime > hlc->dateTime) ? currentWallTime : hlc->dateTime;
    unsigned short counterNew = hlc->counter + 1;
 
    // if (currentWallTime > hlc->dateTime) {
    //     dateTimeNew = currentWallTime;
    //     counterNew = 0;
    // } else if (currentWallTime == hlc->dateTime) {
    //     dateTimeNew = hlc->dateTime;
    //     counterNew = hlc->counter + 1;
    // } else {
    //     dateTimeNew = hlc->dateTime;
    //     counterNew = hlc->counter + 1;
    // }

    // if (dateTimeNew - currentWallTime > MAX_DRIFT) {
    //     return NULL; // Clock drift
    // }
    if (counterNew > MAX_COUNTER) {
        return NULL; // Overflow
    }

    Hlc* newHlc = (Hlc*)malloc(sizeof(Hlc));
    if (newHlc == NULL) {
        return NULL;
    }
    newHlc->dateTime = hlc->dateTime;
    newHlc->counter = counterNew;
    strncpy(newHlc->nodeId, hlc->nodeId, MAX_NODE_ID_LENGTH - 1);
    newHlc->nodeId[MAX_NODE_ID_LENGTH - 1] = '\0';
    return newHlc;
}

// Method: merge(Hlc remote, {DateTime? wallTime})
static Hlc* hlc_merge(const Hlc* local, const Hlc* remote, int64_t wallTimeMillis) {
    if (local == NULL || remote == NULL) {
        return NULL;
    }
    int64_t currentWallTime = (wallTimeMillis != -1) ? wallTimeMillis : getCurrentUtcMillis();

    if (remote->dateTime < local->dateTime ||
        (remote->dateTime == local->dateTime && remote->counter <= local->counter)) {
        return hlc_apply(local, -1, (unsigned short)-1, NULL); // Return a copy
    }

    if (strcmp(local->nodeId, remote->nodeId) == 0) {
        return NULL; // Duplicate node
    }

    if (remote->dateTime - currentWallTime > MAX_DRIFT) {
        return NULL; // Remote clock drift
    }

    Hlc* mergedHlc = hlc_apply(remote, -1, (unsigned short)-1, local->nodeId);
    if (mergedHlc == NULL) {
        return NULL;
    }

    int64_t newDateTime = (currentWallTime > remote->dateTime) ? currentWallTime : remote->dateTime;
    unsigned short newCounter = (newDateTime == remote->dateTime) ? remote->counter : 0;

    Hlc* finalHlc = hlc_apply(mergedHlc, newDateTime, newCounter, local->nodeId);
    free(mergedHlc);
    return finalHlc;
}

// Method: toString()
static char* hlc_str(const Hlc* hlc) {
    if (hlc == NULL) {
        return NULL;
    }
    struct tm tm;
    time_t t = hlc->dateTime / 1000;
#ifdef _WIN32
    errno_t err = gmtime_s(&tm, &t);
    if (err != 0) {
        return NULL;
    }
#else
    if (gmtime_r(&t, &tm) == NULL) {
        return NULL;
    }
#endif

    char dateTimeStr[32];
    strftime(dateTimeStr, sizeof(dateTimeStr), "%Y-%m-%dT%H:%M:%S", &tm);

    char counterStr[5];
    snprintf(counterStr, sizeof(counterStr), "%04X", hlc->counter);

    size_t bufferSize = strlen(dateTimeStr) + 4 + 1 + 4 + 1 + strlen(hlc->nodeId) + 1;
    char* result = (char*)malloc(bufferSize);
    if (result == NULL) {
        return NULL;
    }
    snprintf(result, bufferSize, "%s.%03lld-%s-%s", dateTimeStr, hlc->dateTime % 1000, counterStr, hlc->nodeId);
    return result;
}

// Method: compareTo(Hlc other)
static int hlc_compareTo(const Hlc* hlc1, const Hlc* hlc2) {
    if (hlc1 == NULL || hlc2 == NULL) {
        return 0; // Or handle error appropriately
    }
    if (hlc1->dateTime == hlc2->dateTime) {
        if (hlc1->counter == hlc2->counter) {
            return strcmp(hlc1->nodeId, hlc2->nodeId);
        } else {
            return (hlc1->counter < hlc2->counter) ? -1 : 1;
        }
    } else {
        return (hlc1->dateTime < hlc2->dateTime) ? -1 : 1;
    }
}

// Function to free the memory allocated for Hlc
static void hlc_free(Hlc* hlc) {
    if (hlc != NULL) {
        free(hlc);
    }
}

// --- SQLite Function Implementations ---

static void sqlite_hlc_now(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_now requires exactly one argument (node_id)", -1);
        return;
    }
    const unsigned char *nodeId = sqlite3_value_text(argv[0]);
    if (nodeId == NULL) {
        sqlite3_result_error(context, "node_id argument must be a text value", -1);
        return;
    }
    Hlc* hlc = hlc_now((const char*)nodeId);
    if (hlc == NULL) {
        sqlite3_result_error(context, "Failed to create HLC", -1);
        return;
    }
    char* hlcStr = hlc_str(hlc);
    hlc_free(hlc);
    if (hlcStr == NULL) {
        sqlite3_result_error(context, "Failed to convert HLC to string", -1);
        return;
    }
    sqlite3_result_text(context, hlcStr, -1, sqlite3_free);
}

static void sqlite_hlc_node_id(sqlite3_context *context, int argc, sqlite3_value **argv) { 
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_node_id requires exactly one argument (hlc_text)", -1);
        return;
    }
    const unsigned char *hlcText = sqlite3_value_text(argv[0]);
    if (hlcText == NULL) {
        sqlite3_result_error(context, "hlc_text argument must be a text value", -1);
        return;
    }
    Hlc* hlc = hlc_parse((const char*)hlcText);
    if (hlc == NULL) {
        sqlite3_result_error(context, "Invalid HLC text provided", -1);
        return;
    }
    char nodeIdStr[MAX_NODE_ID_LENGTH];
    strncpy(nodeIdStr, hlc->nodeId, MAX_NODE_ID_LENGTH - 1);
    nodeIdStr[MAX_NODE_ID_LENGTH - 1] = '\0';

    sqlite3_result_text(context, nodeIdStr, -1, SQLITE_STATIC);
    hlc_free(hlc);
}

static void sqlite_hlc_counter(sqlite3_context *context, int argc, sqlite3_value **argv) { 
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_counter requires exactly one argument (hlc_text)", -1);
        return;
    }
    const unsigned char *hlcText = sqlite3_value_text(argv[0]);
    if (hlcText == NULL) {
        sqlite3_result_error(context, "hlc_text argument must be a text value", -1);
        return;
    }
    Hlc* hlc = hlc_parse((const char*)hlcText);
    if (hlc == NULL) {
        sqlite3_result_error(context, "Invalid HLC text provided", -1);
        return;
    }
    sqlite3_result_int(context, hlc->counter);
    hlc_free(hlc);
}

static void sqlite_hlc_date_time(sqlite3_context *context, int argc, sqlite3_value **argv) { 
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_date_time requires exactly one argument (hlc_text)", -1);
        return;
    }
    const unsigned char *hlcText = sqlite3_value_text(argv[0]);
    if (hlcText == NULL) {
        sqlite3_result_error(context, "hlc_text argument must be a text value", -1);
        return;
    }
    Hlc* hlc = hlc_parse((const char*)hlcText);
    if (hlc == NULL) {
        sqlite3_result_error(context, "Invalid HLC text provided", -1);
        return;
    }
    sqlite3_result_int64(context, hlc->dateTime);
    hlc_free(hlc);
}

static void sqlite_hlc_parse(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_parse requires exactly one argument (timestamp)", -1);
        return;
    }
    const unsigned char *timestamp = sqlite3_value_text(argv[0]);
    if (timestamp == NULL) {
        sqlite3_result_error(context, "timestamp argument must be a text value", -1);
        return;
    }
    Hlc* hlc = hlc_parse((const char*)timestamp);
    if (hlc == NULL) {
        sqlite3_result_error(context, "Failed to parse HLC string", -1);
        return;
    }
    char* hlcStr = hlc_str(hlc);
    hlc_free(hlc);
    if (hlcStr == NULL) {
        sqlite3_result_error(context, "Failed to convert parsed HLC to string", -1);
        return;
    }
    sqlite3_result_text(context, hlcStr, -1, sqlite3_free);
}

static void sqlite_hlc_increment(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1 && argc != 0) {
        sqlite3_result_error(context, "hlc_increment requires zero or one argument (hlc_text)", -1);
        return;
    }
    const unsigned char *hlcText = sqlite3_value_text(argv[0]);
    if (hlcText == NULL && argc == 1) {
        sqlite3_result_error(context, "hlc_text argument must be a text value", -1);
        return;
    }

    Hlc* hlc;
    if (argc == 1) {
        hlc = hlc_parse((const char*)hlcText);
        if (hlc == NULL) {
            sqlite3_result_error(context, "Invalid HLC text provided", -1);
            return;
        }
    } else {
        sqlite3_result_error(context, "hlc_increment without argument needs the context of the current node ID, which is not yet implemented in this example.", -1);
        return;
    }
    Hlc* incrementedHlc = hlc_increment(hlc, -1);
    hlc_free(hlc);
    if (incrementedHlc == NULL) {
        sqlite3_result_error(context, "Failed to increment HLC (potential overflow or drift)", -1);
        return;
    }
    char* incrementedHlcStr = hlc_str(incrementedHlc);
    hlc_free(incrementedHlc);
    if (incrementedHlcStr == NULL) {
        sqlite3_result_error(context, "Failed to convert incremented HLC to string", -1);
        return;
    }
    sqlite3_result_text(context, incrementedHlcStr, -1, sqlite3_free);
}

static void sqlite_hlc_merge(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "hlc_merge requires exactly two arguments (local_hlc_text, remote_hlc_text)", -1);
        return;
    }
    const unsigned char *localHlcText = sqlite3_value_text(argv[0]);
    const unsigned char *remoteHlcText = sqlite3_value_text(argv[1]);

    if (localHlcText == NULL || remoteHlcText == NULL) {
        sqlite3_result_error(context, "HLC text arguments must be text values", -1);
        return;
    }

    Hlc* localHlc = hlc_parse((const char*)localHlcText);
    Hlc* remoteHlc = hlc_parse((const char*)remoteHlcText);

    if (localHlc == NULL || remoteHlc == NULL) {
        sqlite3_result_error(context, "Invalid HLC text provided for merging", -1);
        hlc_free(localHlc);
        hlc_free(remoteHlc);
        return;
    }

    Hlc* mergedHlc = hlc_merge(localHlc, remoteHlc, -1);
    hlc_free(localHlc);
    hlc_free(remoteHlc);

    if (mergedHlc == NULL) {
        sqlite3_result_error(context, "Failed to merge HLCs (potential duplicate node or drift)", -1);
        return;
    }

    char* mergedHlcStr = hlc_str(mergedHlc);
    hlc_free(mergedHlc);

    if (mergedHlcStr == NULL) {
        sqlite3_result_error(context, "Failed to convert merged HLC to string", -1);
        return;
    }

    sqlite3_result_text(context, mergedHlcStr, -1, sqlite3_free);
}

static void sqlite_hlc_str(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "hlc_str requires exactly one argument (hlc_text)", -1);
        return;
    }
    const unsigned char *hlcText = sqlite3_value_text(argv[0]);
    if (hlcText == NULL) {
        sqlite3_result_error(context, "hlc_text argument must be a text value", -1);
        return;
    }
    sqlite3_result_text(context, (const char*)hlcText, -1, SQLITE_STATIC);
}

static void sqlite_hlc_compare(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "hlc_compare requires exactly two arguments (hlc_text1, hlc_text2)", -1);
        return;
    }
    const unsigned char *hlcText1 = sqlite3_value_text(argv[0]);
    const unsigned char *hlcText2 = sqlite3_value_text(argv[1]);

    if (hlcText1 == NULL || hlcText2 == NULL) {
        sqlite3_result_error(context, "HLC text arguments must be text values", -1);
        return;
    }

    Hlc* hlc1 = hlc_parse((const char*)hlcText1);
    Hlc* hlc2 = hlc_parse((const char*)hlcText2);

    if (hlc1 == NULL || hlc2 == NULL) {
        sqlite3_result_error(context, "Invalid HLC text provided for comparison", -1);
        hlc_free(hlc1);
        hlc_free(hlc2);
        return;
    }

    int comparisonResult = hlc_compareTo(hlc1, hlc2);
    hlc_free(hlc1);
    hlc_free(hlc2);

    sqlite3_result_int(context, comparisonResult);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_hlc_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;  /* Unused parameter */

    rc = sqlite3_create_function(db, "hlc_now", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS, NULL, sqlite_hlc_now, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_node_id", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_node_id, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_counter", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_counter, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_date_time", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_date_time, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
   
    rc = sqlite3_create_function(db, "hlc_parse", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_parse, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_increment", 1, SQLITE_UTF8 | SQLITE_INNOCUOUS, NULL, sqlite_hlc_increment, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_merge", 2, SQLITE_UTF8 | SQLITE_INNOCUOUS, NULL, sqlite_hlc_merge, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_str", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_str, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "hlc_compare", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL, sqlite_hlc_compare, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    return SQLITE_OK;
}