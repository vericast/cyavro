from libc.stdio cimport FILE
from libc.stddef cimport wchar_t
from posix.types cimport off_t

cdef extern from "stdio.h" nogil:
    # File descriptors
    FILE *fdopen(int, const char *)
    int fileno(FILE *)

    # Pipes
    FILE *popen(const char *, const char *)
    int pclose(FILE *)

    # Seek and tell with off_t
    int fseeko(FILE *, off_t, int)
    off_t ftello(FILE *)

    # Locking (for multithreading)
    void flockfile(FILE *)
    int ftrylockfile(FILE *)
    void funlockfile(FILE *)
    int getc_unlocked(FILE *)
    int getchar_unlocked()
    int putc_unlocked(int, FILE *)
    int putchar_unlocked(int)

    # Reading lines and records (POSIX.2008)
    ssize_t getline(char **, size_t *, FILE *)
    ssize_t getdelim(char **, size_t *, int, FILE *)


IF UNAME_SYSNAME == "Linux":
    cdef extern from "stdio.h" nogil:
        # Memory streams (POSIX.2008)
        FILE *fmemopen(void *, size_t, const char *)
        FILE *open_memstream(char **, size_t *)
        FILE *open_wmemstream(wchar_t **, size_t *)

ELIF UNAME_SYSNAME == "Darwin":
    cdef extern from "osx/fmemopen.h" nogil:
        FILE *fmemopen(void *, size_t, const char *)
