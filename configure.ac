AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(mc-hammer, VERSION_NUMBER, trond.norbye@gmail.com)
AC_CONFIG_SRCDIR([hammer.cc])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_DISABLE_STATIC

PANDORA_CANONICAL_TARGET(less-warnings, warnings-always-on, require-cxx,
                         force-gcc42)
PANDORA_REQUIRE_LIBMEMCACHED

AC_ARG_WITH([memcached],
    [AS_HELP_STRING([--with-memcached],
      [Memcached source location])],
    [ac_cv_with_memcached="$withval"],
    [ac_cv_with_memcached=""])

AS_IF(test "x${ac_cv_with_memcached}" != "x",
      [CPPFLAGS="-I${ac_cv_with_memcached}/include $CPPFLAGS"
       PATH="${ac_cv_with_memcached}:$PATH"])

AC_CHECK_HEADERS(memcached/protocol_binary.h)
AS_IF(test "x${ac_cv_header_memcached_protocol_binary_h}" != "xyes",
      AC_MSG_ERROR([Can't build molly without memcached/protocol_binary.h]))

AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile)
AC_OUTPUT
echo "---"
echo "Configuration summary for $PACKAGE_NAME version $VERSION"
echo ""
echo "   * Installation prefix:       $prefix"
echo "   * System type:               $host_vendor-$host_os"
echo "   * Host CPU:                  $host_cpu"
echo "   * C Compiler:                $CC_VERSION"
echo "   * C++ Compiler:              $CXX_VERSION"
echo "   * Assertions enabled:        $ac_cv_assert"
echo "   * Debug enabled:             $with_debug"
echo "   * Warnings as failure:       $ac_cv_warnings_as_errors"
echo ""
echo "---"
