#define OTL_ORA9I // Compile OTL 4.0/OCI9i
// #define OTL_ORA8
// #define OTL_ORA8I

#if defined(_MSC_VER) // VC++

// Enabling support for 64-bit signed integers
// Since 64-bit integers are not part of the ANSI C++
// standard, this definition is compiler specific.
#define OTL_BIGINT __int64

// Defining a bigint constant that is larger than
// the max 32-bit integer value.
const OTL_BIGINT BIGINT_VAL1=12345678901234000;

// Defining a string-to-bigint conversion 
// that is used by OTL internally.
// Since 64-bit ineteger conversion functions are
// not part of the ANSI C++ standard, the code
// below is compiler specific
#define OTL_STR_TO_BIGINT(str,n)                \
{                                               \
  n=_atoi64(str);                               \
}

// Defining a bigint-to-string conversion 
// that is used by OTL internally.
// Since 64-bit ineteger conversion functions are
// not part of the ANSI C++ standard, the code
// below is compiler specific
#define OTL_BIGINT_TO_STR(n,str)                \
{                                               \
  _i64toa(n,str,10);                            \
}

#elif defined(__GNUC__) // GNU C++

#include <stdlib.h>

// Enabling support for 64-bit signed integers
// Since 64-bit integers are not part of the ANSI C++
// standard, this definition is compiler specific.
#define OTL_BIGINT long long

const OTL_BIGINT BIGINT_VAL1=12345678901234000LL;

// Defining a string-to-bigint conversion 
// that is used by OTL internally.
// Since 64-bit ineteger conversion functions are
// not part of the ANSI C++ standard, the code
// below is compiler specific.
#define OTL_STR_TO_BIGINT(str,n)                \
{                                               \
  n=strtoll(str,0,10);                          \
}

// Defining a bigint-to-string conversion 
// that is used by OTL internally.
// Since 64-bit ineteger conversion functions are
// not part of the ANSI C++ standard, the code
// below is compiler specific
#define OTL_BIGINT_TO_STR(n,str)                \
{                                               \
  sprintf(str,"%lld",n);                        \
}


#endif

#define OTL_STL
#include "otlv4.h"
