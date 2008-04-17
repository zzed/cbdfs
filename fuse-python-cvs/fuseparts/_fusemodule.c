/*
    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>

    This program can be distributed under the terms of the GNU LGPL.
    See the file COPYING.

    Updated for libfuse API changes
    2004 Steven James <pyro@linuxlabs.com> and
    Linux Labs International, Inc. http://www.linuxlabs.com

    Copyright (C) 2006-2007  Csaba Henk  <csaba.henk@creo.hu> 
*/

/* 
 * Local Variables:
 * indent-tabs-mode: t
 * c-basic-offset: 8
 * End:
 * Changed by David McNab (david@rebirthing.co.nz) to work with recent pythons.
 * Namely, replacing PyTuple_* with PySequence_*, and checking numerical values
 * with both PyInt_Check and PyLong_Check.
 */

#ifndef FUSE_VERSION
#ifndef FUSE_MAKE_VERSION
#define FUSE_MAKE_VERSION(maj, min)  ((maj) * 10 + (min))
#endif
#define FUSE_VERSION FUSE_MAKE_VERSION(FUSE_MAJOR_VERSION, FUSE_MINOR_VERSION)
#endif

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

#include <Python.h>
#include <fuse.h>

static PyObject *getattr_cb=NULL, *readlink_cb=NULL, *readdir_cb=NULL,
  *mknod_cb=NULL, *mkdir_cb=NULL, *unlink_cb=NULL, *rmdir_cb=NULL,
  *symlink_cb=NULL, *rename_cb=NULL, *link_cb=NULL, *chmod_cb=NULL,
  *chown_cb=NULL, *truncate_cb=NULL, *utime_cb=NULL,
  *open_cb=NULL, *read_cb=NULL, *write_cb=NULL, *release_cb=NULL,
  *statfs_cb=NULL, *fsync_cb=NULL, *create_cb=NULL, *opendir_cb=NULL,
  *releasedir_cb=NULL, *fsyncdir_cb=NULL, *flush_cb=NULL, *ftruncate_cb=NULL,
  *fgetattr_cb=NULL, *getxattr_cb=NULL, *listxattr_cb=NULL, *setxattr_cb=NULL,
  *removexattr_cb=NULL, *access_cb=NULL, *lock_cb = NULL, *utimens_cb = NULL,
  *bmap_cb = NULL, *fsinit_cb=NULL, *fsdestroy_cb = NULL;

static PyObject *Py_FuseError;
static PyInterpreterState *interp;

#ifdef WITH_THREAD

#define PYLOCK()						\
PyThreadState *_state = NULL;					\
if (interp) {							\
	PyEval_AcquireLock();					\
	_state = PyThreadState_New(interp);			\
	PyThreadState_Swap(_state);				\
}

#define PYUNLOCK() if (interp) {				\
	PyThreadState_Clear(_state);				\
	PyThreadState_Swap(NULL);				\
	PyThreadState_Delete(_state);				\
	PyEval_ReleaseLock();					\
}
 
#else
#define PYLOCK()
#define PYUNLOCK()
#endif /* WITH_THREAD */

#define PROLOGUE(pyval)		\
int ret = -EINVAL;		\
PyObject *v;			\
				\
PYLOCK();			\
				\
v = pyval;			\
				\
if (!v) {			\
	PyErr_Print();		\
	goto OUT;		\
}				\
if (v == Py_None) {		\
	ret = 0;		\
	goto OUT_DECREF;	\
}				\
if (PyInt_Check(v)) {		\
	ret = PyInt_AsLong(v);	\
	goto OUT_DECREF;	\
}

#define EPILOGUE		\
OUT_DECREF:			\
	Py_DECREF(v);		\
OUT:				\
	PYUNLOCK();		\
	return ret;

#if FUSE_VERSION >= 22
static __inline PyObject *
fi_to_py(struct fuse_file_info *fi)
{
	return (PyObject *)(uintptr_t)fi->fh;
}

#define PYO_CALLWITHFI(fi, fnc, fmt, ...)				      \
	fi_to_py(fi) ?							      \
	PyObject_CallFunction(fnc, #fmt "O", ## __VA_ARGS__, fi_to_py(fi)) :  \
	PyObject_CallFunction(fnc, #fmt, ## __VA_ARGS__)
#else
#define PYO_CALLWITHFI(fi, fnc, fmt, ...)				      \
	PyObject_CallFunction(fnc, #fmt, ## __VA_ARGS__)
#endif /* FUSE_VERSION >= 22 */


/* transform a Python integer to an unsigned C numeric value */

#define py2attr(st, attr)						\
	if (PyInt_Check(pytmp) && sizeof((st)->attr) <= sizeof(long)) {	\
		/*							\
		 * We'd rather use here PyInt_AsUnsignedLong() here	\
		 * but there is no such thing. Closest match is		\
		 * PyInt_AsUnsignedLongMask() but that doesn't check	\
		 * for overflows. Duh.					\
		 */							\
		ctmp = PyInt_AsLong(pytmp);				\
		if (ctmp >						\
		    /* damn the funcall overhead...			\
		           PyInt_GetMax() */				\
		           LONG_MAX) {					\
			/*						\
			 * If the value, as unsigned, is bigger than	\
			 * Python ints can be, then it was a negative	\
			 * integer so bail out.				\
			 */						\
			Py_DECREF(pytmp);				\
			goto OUT_DECREF;				\
		}							\
	} else {							\
		if (PyInt_Check(pytmp))					\
			/*						\
			 * This fnc doesn't catch overflows but I guess	\
			 * it shouldn't overflow after passing		\
			 * PyInt_Check() ...				\
			 */						\
			ctmp = PyInt_AsUnsignedLongLongMask(pytmp);	\
		else if (PyLong_Check(pytmp))				\
			ctmp = PyLong_AsUnsignedLongLong(pytmp);	\
		else if (PyFloat_Check(pytmp))				\
			ctmp =						\
			  (unsigned long long)PyFloat_AsDouble(pytmp);	\
		else {							\
			Py_DECREF(pytmp);				\
			goto OUT_DECREF;				\
		}							\
	}								\
	Py_DECREF(pytmp);						\
	if (PyErr_Occurred())						\
		goto OUT_DECREF;					\
	(st)->attr = ctmp;						\
	if ((unsigned long long)(st)->attr != ctmp)			\
		goto OUT_DECREF;
		
#define fetchattr_nam(st, attr, aname)					\
	if (!(pytmp = PyObject_GetAttrString(v, aname)))		\
		goto OUT_DECREF;					\
	py2attr(st, attr);

#define fetchattr(st, attr)						\
	fetchattr_nam(st, attr, #attr)

#define fetchattr_soft(st, attr)					\
	if (PyObject_HasAttrString(v, #attr)) {				\
		fetchattr(st, attr);					\
	}

/*
 * Following macros are only for getattr-alikes, we undef them after
 * the getattr type functions.
 */

#define fetchattr_soft_d(st, attr, defa)				\
	fetchattr_soft(st, attr) else st->attr = defa

#define FETCH_STAT_DATA()						\
	fetchattr(st, st_mode);						\
	fetchattr(st, st_ino);						\
	fetchattr(st, st_dev);						\
	fetchattr(st, st_nlink);					\
	fetchattr(st, st_uid);						\
	fetchattr(st, st_gid);						\
	fetchattr(st, st_size);						\
	fetchattr(st, st_atime);					\
	fetchattr(st, st_mtime);					\
	fetchattr(st, st_ctime);					\
									\
	/*								\
	 * Following fields are not necessarily available on all	\
	 * platforms (were "all" stands for "POSIX-like"). Therefore	\
	 * we should have some #ifdef-s around... However, they _are_	\
	 * available on those platforms where FUSE has a chance to	\
	 * run now and in the foreseeable future, and we don't use	\
	 * autotools so we just dare to throw these in as is.		\
	 */								\
									\
	fetchattr_soft(st, st_rdev);					\
	fetchattr_soft_d(st, st_blksize, 4096);				\
	fetchattr_soft_d(st, st_blocks, (st->st_size + 511)/512)


static int
getattr_func(const char *path, struct stat *st)
{
	PyObject *pytmp;
	unsigned long long ctmp;

	PROLOGUE( PyObject_CallFunction(getattr_cb, "s", path) )

	FETCH_STAT_DATA();

	ret = 0;

	EPILOGUE
}

#if FUSE_VERSION >= 25
static int
fgetattr_func(const char *path, struct stat *st, struct fuse_file_info *fi)
{
	PyObject *pytmp;
	unsigned long long ctmp;

	PROLOGUE( PYO_CALLWITHFI(fi, fgetattr_cb, s, path) )

	FETCH_STAT_DATA();

	ret = 0;

	EPILOGUE

}
#endif

#undef fetchattr_soft_d
#undef FETCH_STAT_DATA

static int
readlink_func(const char *path, char *link, size_t size)
{
	char *s;

	PROLOGUE( PyObject_CallFunction(readlink_cb, "s", path) )

	if(!PyString_Check(v)) {
		ret = -EINVAL;
		goto OUT_DECREF;
	}
	s = PyString_AsString(v);
	strncpy(link, s, size);
	link[size-1] = '\0';
	ret = 0;

	EPILOGUE
}

#if FUSE_VERSION >= 23
static int
opendir_func(const char *path, struct fuse_file_info *fi)
{
	PROLOGUE( PyObject_CallFunction(opendir_cb, "s", path) )

	fi->fh = (uintptr_t) v;

	return 0;

	EPILOGUE
}

static int
releasedir_func(const char *path, struct fuse_file_info *fi)
{
	PROLOGUE(
	  fi_to_py(fi) ?
  	  PyObject_CallFunction(releasedir_cb, "sN", path,
	                        fi_to_py(fi)) :
	  PyObject_CallFunction(releasedir_cb, "s", path)
	)

	EPILOGUE
}

static int
fsyncdir_func(const char *path, int datasync, struct fuse_file_info *fi)
{
	PROLOGUE( PYO_CALLWITHFI(fi, fsyncdir_cb, si, path, datasync) )
	EPILOGUE
}

static __inline int
dir_add_entry(PyObject *v, void *buf, fuse_fill_dir_t df)
#else
static __inline int
dir_add_entry(PyObject *v, fuse_dirh_t buf, fuse_dirfil_t df)
#endif
{
	PyObject *pytmp;
	unsigned long long ctmp;
	int ret = -EINVAL;
	struct stat st;
	struct { off_t offset; } offs;

	memset(&st, 0, sizeof(st));
	fetchattr_nam(&st, st_ino, "ino");
	fetchattr_nam(&st, st_mode, "type");
	fetchattr(&offs, offset);

	if (!(pytmp = PyObject_GetAttrString(v, "name"))) 
		goto OUT_DECREF;		       
	if (!PyString_Check(pytmp)) {
		Py_DECREF(pytmp);
		goto OUT_DECREF;		       
	}					       

#if FUSE_VERSION >= 23
	ret = df(buf, PyString_AsString(pytmp), &st, offs.offset);
#elif FUSE_VERSION >= 21
	ret = df(buf, PyString_AsString(pytmp), (st.st_mode & 0170000) >> 12,
                 st.st_ino);
#else
	ret = df(buf, PyString_AsString(pytmp), (st.st_mode & 0170000) >> 12);
#endif
	Py_DECREF(pytmp);

OUT_DECREF:
	Py_DECREF(v);

	return ret;
}

#if FUSE_VERSION >= 23
static int
readdir_func(const char *path, void *buf, fuse_fill_dir_t df, off_t off,
             struct fuse_file_info *fi)
{
	PyObject *iter, *w;

	PROLOGUE( PYO_CALLWITHFI(fi, readdir_cb, sK, path, off) )
#else
static int
readdir_func(const char *path, fuse_dirh_t buf, fuse_dirfil_t df)
{
	PyObject *iter, *w;

	PROLOGUE( PyObject_CallFunction(readdir_cb, "sK", path) )
#endif

	iter = PyObject_GetIter(v);
	if(!iter) {
		PyErr_Print();
		goto OUT_DECREF;
	}

	while ((w = PyIter_Next(iter))) {
		if (dir_add_entry(w, buf, df))
			break;
	}

	Py_DECREF(iter);
	if (PyErr_Occurred()) {
		PyErr_Print();
		goto OUT_DECREF;
	}
	ret = 0;

	EPILOGUE
}

static int
mknod_func(const char *path, mode_t m, dev_t d)
{
	PROLOGUE( PyObject_CallFunction(mknod_cb, "sii", path, m, d) )
	EPILOGUE
}

static int
mkdir_func(const char *path, mode_t m)
{
	PROLOGUE( PyObject_CallFunction(mkdir_cb, "si", path, m) )
	EPILOGUE
}

static int
unlink_func(const char *path)
{
	PROLOGUE( PyObject_CallFunction(unlink_cb, "s", path) )
	EPILOGUE
}

static int
rmdir_func(const char *path)
{
	PROLOGUE( PyObject_CallFunction(rmdir_cb, "s", path) )
	EPILOGUE
}

static int
symlink_func(const char *path, const char *path1)
{
	PROLOGUE( PyObject_CallFunction(symlink_cb, "ss", path, path1) )
	EPILOGUE
}

static int
rename_func(const char *path, const char *path1)
{
	PROLOGUE( PyObject_CallFunction(rename_cb, "ss", path, path1) )
	EPILOGUE
}

static int
link_func(const char *path, const char *path1)
{
	PROLOGUE( PyObject_CallFunction(link_cb, "ss", path, path1) )
	EPILOGUE
}

static int
chmod_func(const char *path, mode_t m) 
{
	PROLOGUE( PyObject_CallFunction(chmod_cb, "si", path, m) )
	EPILOGUE
}

static int
chown_func(const char *path, uid_t u, gid_t g) 
{
	PROLOGUE( PyObject_CallFunction(chown_cb, "sii", path, u, g) )
	EPILOGUE
}

static int
truncate_func(const char *path, off_t length)
{
	PROLOGUE( PyObject_CallFunction(truncate_cb, "sK", path, length) )
	EPILOGUE
}

#if FUSE_VERSION >= 25
static int
ftruncate_func(const char *path, off_t length, struct fuse_file_info *fi)
{
	PROLOGUE( PYO_CALLWITHFI(fi, ftruncate_cb, sK, path, length) )
	EPILOGUE
}
#endif

static int
utime_func(const char *path, struct utimbuf *u)
{
	int actime = u ? u->actime : time(NULL);
	int modtime = u ? u->modtime : actime;
	PROLOGUE(
	  PyObject_CallFunction(utime_cb, "s(ii)", path, actime, modtime)
	)
	EPILOGUE
}

#if FUSE_VERSION >= 22
static int
read_func(const char *path, char *buf, size_t s, off_t off,
                     struct fuse_file_info *fi)
#else
static int
read_func(const char *path, char *buf, size_t s, off_t off)
#endif
{
#if PY_VERSION_HEX < 0x02050000
	PROLOGUE( PYO_CALLWITHFI(fi, read_cb, siK, path, s, off) )
#else
	PROLOGUE( PYO_CALLWITHFI(fi, read_cb, snK, path, s, off) )
#endif

	if(PyString_Check(v)) {
		if(PyString_Size(v) > s)
			goto OUT_DECREF;
		memcpy(buf, PyString_AsString(v), PyString_Size(v));
		ret = PyString_Size(v);
	}

	EPILOGUE
}

#if FUSE_VERSION >= 22
static int
write_func(const char *path, const char *buf, size_t t, off_t off,
           struct fuse_file_info *fi)
#else
static int
write_func(const char *path, const char *buf, size_t t, off_t off)
#endif
{
	PROLOGUE( PYO_CALLWITHFI(fi, write_cb, ss#K, path, buf, t, off) )
	EPILOGUE
}

#if FUSE_VERSION >= 22
static int
open_func(const char *path, struct fuse_file_info *fi)
{
	PyObject *pytmp, *pytmp1;

	PROLOGUE( PyObject_CallFunction(open_cb, "si", path, fi->flags) )

	pytmp = PyTuple_GetItem(v, 0);

#if FUSE_VERSION >= 23
	pytmp1 = PyObject_GetAttrString(pytmp, "keep_cache");
	if (pytmp1) {
		fi->keep_cache = PyObject_IsTrue(pytmp1);
		Py_DECREF(pytmp1);
	}
	pytmp1 = PyObject_GetAttrString(pytmp, "direct_io");
	if (pytmp1) {
		fi->direct_io = PyObject_IsTrue(pytmp1);
		Py_DECREF(pytmp1);
	}

	if (PyObject_IsTrue(PyTuple_GetItem(v, 1)))
#endif
	{
		Py_INCREF(pytmp);
		fi->fh = (uintptr_t) pytmp;
	}

	ret = 0;
	goto OUT;

	EPILOGUE
}
#else
static int
open_func(const char *path, int mode)
{
	PROLOGUE( PyObject_CallFunction(open_cb, "si", path, mode) )
	EPILOGUE
}
#endif

#if FUSE_VERSION >= 25
static int
create_func(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	PyObject *pytmp, *pytmp1;

	PROLOGUE(
	  PyObject_CallFunction(create_cb, "sii", path, fi->flags, mode)
	)

	pytmp = PyTuple_GetItem(v, 0);

	pytmp1 = PyObject_GetAttrString(pytmp, "keep_cache");
	if (pytmp1) {
		fi->keep_cache = PyObject_IsTrue(pytmp1);
		Py_DECREF(pytmp1);
	}
	pytmp1 = PyObject_GetAttrString(pytmp, "direct_io");
	if (pytmp1) {
		fi->direct_io = PyObject_IsTrue(pytmp1);
		Py_DECREF(pytmp1);
	}

	if (PyObject_IsTrue(PyTuple_GetItem(v, 1))) {
		Py_INCREF(pytmp);
		fi->fh = (uintptr_t) pytmp;
	}

	ret = 0;
	goto OUT;

	EPILOGUE
}
#endif

#if FUSE_VERSION >= 22
static int
release_func(const char *path, struct fuse_file_info *fi)
{
	PROLOGUE(
	  fi_to_py(fi) ?
	  PyObject_CallFunction(release_cb, "siN", path, fi->flags,
	                        fi_to_py(fi)) :
	  PyObject_CallFunction(release_cb, "si", path, fi->flags)
	)
#else
static int
release_func(const char *path, int flags)
{
	PROLOGUE( PyObject_CallFunction(release_cb, "si", path, flags) )
#endif
	EPILOGUE
}

#if FUSE_VERSION >= 25
static int
statfs_func(const char *dummy, struct statvfs *fst)
#else
static int
statfs_func(const char *dummy, struct statfs *fst)
#endif
{
	PyObject *pytmp;
	unsigned long long ctmp;
	PROLOGUE( PyObject_CallFunction(statfs_cb, "") )

	fetchattr(fst, f_bsize);
#if FUSE_VERSION >= 25
	fetchattr(fst, f_frsize);
#endif
	fetchattr(fst, f_blocks);
	fetchattr(fst, f_bfree);
	fetchattr(fst, f_bavail);
	fetchattr(fst, f_files);
	fetchattr(fst, f_ffree);
#if FUSE_VERSION >= 25
	fetchattr(fst, f_favail);
	fetchattr(fst, f_flag);
	fetchattr(fst, f_namemax);
#else
	fetchattr_nam(fst, f_namelen, "f_namemax");
#endif

	ret = 0;
 
	EPILOGUE
}

#if FUSE_VERSION >= 22
static int
fsync_func(const char *path, int datasync, struct fuse_file_info *fi)
#else
static int
fsync_func(const char *path, int datasync)
#endif
{
	PROLOGUE( PYO_CALLWITHFI(fi, fsync_cb, si, path, datasync) )
	EPILOGUE
}

#if FUSE_VERSION >= 22
static int
flush_func(const char *path, struct fuse_file_info *fi)
#else
static int
flush_func(const char *path)
#endif
{
	PROLOGUE( PYO_CALLWITHFI(fi, flush_cb, s, path) )
	EPILOGUE
}

static int
getxattr_func(const char *path, const char *name, char *value, size_t size)
{
#if PY_VERSION_HEX < 0x02050000
	PROLOGUE( PyObject_CallFunction(getxattr_cb, "ssi", path, name, size) )
#else
	PROLOGUE( PyObject_CallFunction(getxattr_cb, "ssn", path, name, size) )
#endif

	if(PyString_Check(v)) {
		if(PyString_Size(v) > size)
			goto OUT_DECREF;
		memcpy(value, PyString_AsString(v), PyString_Size(v));
		ret = PyString_Size(v);
	}

	EPILOGUE
}

static int
listxattr_func(const char *path, char *list, size_t size)
{
	PyObject *iter, *w;
	char *lx = list;
#if PY_VERSION_HEX < 0x02050000
	PROLOGUE( PyObject_CallFunction(listxattr_cb, "si", path, size) )
#else
	PROLOGUE( PyObject_CallFunction(listxattr_cb, "sn", path, size) )
#endif
	iter = PyObject_GetIter(v);
	if(!iter) {
		PyErr_Print();
		goto OUT_DECREF;
	}

	for (;;) {
		int ilen;

	        w = PyIter_Next(iter);
		if (!w) {
			ret = lx - list;
			break;
		}

		if (!PyString_Check(w)) {
			Py_DECREF(w);
			break;
		}

		ilen = PyString_Size(w);
		if (lx - list + ilen >= size) {
			Py_DECREF(w);
			break;
		}

		strncpy(lx, PyString_AsString(w), ilen + 1);
		lx += ilen + 1;

		Py_DECREF(w);
	}

	Py_DECREF(iter);
	if (PyErr_Occurred()) {
		PyErr_Print();
		ret = -EINVAL;
	}

	EPILOGUE
}

static int
setxattr_func(const char *path, const char *name, const char *value,
              size_t size, int flags)
{
	PROLOGUE(
	  PyObject_CallFunction(setxattr_cb, "sss#i", path, name, value, size,
	                        flags)
	)
	EPILOGUE
}

static int
removexattr_func(const char *path, const char *name)
{
	PROLOGUE( PyObject_CallFunction(removexattr_cb, "ss", path, name) )
	EPILOGUE
}

#if FUSE_VERSION >= 25
static int
access_func(const char *path, int mask)
{
	PROLOGUE( PyObject_CallFunction(access_cb, "si", path, mask) )
	EPILOGUE
}
#endif

#if FUSE_VERSION >= 23
#if FUSE_VERSION >= 26
static void *
fsinit_func(struct fuse_conn_info *conn)
{
	(void)conn;
#else
static void *
fsinit_func(void)
{
#endif
	PYLOCK();
	PyObject_CallFunction(fsinit_cb, "");
	PYUNLOCK();

	return NULL;
}

static void
fsdestroy_func(void *param)
{
	(void)param;

	PYLOCK();
	PyObject_CallFunction(fsdestroy_cb, "");
	PYUNLOCK();
}
#endif

#if FUSE_VERSION >= 26
static inline PyObject *
lock_func_i(const char *path, struct fuse_file_info *fi, int cmd,
            struct flock *lock)
{
	PyObject *pyargs, *pykw = NULL, *v = NULL;

	pyargs =
	fi_to_py(fi) ?
	Py_BuildValue("(siKO)", path, cmd, fi->lock_owner, fi_to_py(fi)) :
	Py_BuildValue("(siK)", path, cmd, fi->lock_owner); 
	if (! pyargs)
		goto out;

	pykw = Py_BuildValue("{sisKsKsi}",
	                     "l_type",  lock->l_type,
	                     "l_start", lock->l_start,
	                     "l_len",   lock->l_len,
	                     "l_pid",   lock->l_pid);
	if (! pykw)
		goto out;

	v = PyObject_Call(lock_cb, pyargs, pykw);

out:
	Py_XDECREF(pyargs);
	Py_XDECREF(pykw);

	return v;
}

static int
lock_func(const char *path, struct fuse_file_info *fi, int cmd,
          struct flock *lock)
{
	PyObject *pytmp;
	unsigned long long ctmp;

	PROLOGUE( lock_func_i(path, fi, cmd, lock) )

	fetchattr_soft(lock, l_type);
	fetchattr_soft(lock, l_start);
	fetchattr_soft(lock, l_len);
	fetchattr_soft(lock, l_pid);

	ret = 0;

	EPILOGUE
}

static int
utimens_func(const char *path, const struct timespec ts[2])
{
	PROLOGUE(
	  PyObject_CallFunction(utimens_cb, "siiii", path,
	                        ts[0].tv_sec, ts[0].tv_nsec,
	                        ts[1].tv_sec, ts[1].tv_nsec)
	)

	EPILOGUE
}

static int
bmap_func(const char *path, size_t blocksize, uint64_t *idx)
{
	PyObject *pytmp;
	unsigned long long ctmp;
	struct { uint64_t idx; } idxwrapper;

	PROLOGUE(
#if PY_VERSION_HEX < 0x02050000
	  PyObject_CallFunction(bmap_cb, "siK", path, blocksize, *idx)
#else
	  PyObject_CallFunction(bmap_cb, "snK", path, blocksize, *idx)
#endif
	)

	/*
	 * We can make use of our py -> C numeric conversion macro with some
	 * customization of the parameters...
	 */
	pytmp = v;
	Py_INCREF(pytmp);
	py2attr(&idxwrapper, idx);

	*idx = idxwrapper.idx;
	ret = 0;

	EPILOGUE
}
#endif

static int
pyfuse_loop_mt(struct fuse *f)
{
	int err = -1;
#ifdef WITH_THREAD
	PyThreadState *save;

	PyEval_InitThreads();
	interp = PyThreadState_Get()->interp;
	save = PyEval_SaveThread();
	err = fuse_loop_mt(f);
	PyEval_RestoreThread(save);
	interp = NULL;
#endif

	return(err);
}

static struct fuse *fuse=NULL;

static PyObject *
Fuse_main(PyObject *self, PyObject *args, PyObject *kw)
{
#if FUSE_VERSION < 26
	int fd;
#endif
	int multithreaded=0, mthp;
	PyObject *fargseq = NULL;
	int err;
	int i;
	char *fmp;
	struct fuse_operations op;
	int fargc;
	char **fargv;

	static char  *kwlist[] = {
		"getattr", "readlink", "readdir", "mknod",
		"mkdir", "unlink", "rmdir", "symlink", "rename",
		"link", "chmod", "chown", "truncate", "utime",
		"open", "read", "write", "release", "statfs", "fsync",
		"create", "opendir", "releasedir", "fsyncdir", "flush",
	        "ftruncate", "fgetattr", "getxattr", "listxattr", "setxattr",
	        "removexattr", "access", "lock", "utimens", "bmap",
		"fsinit", "fsdestroy", "fuse_args", "multithreaded", NULL
	};
	
	memset(&op, 0, sizeof(op));

	if (!PyArg_ParseTupleAndKeywords(args, kw,
	                                 "|OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOi", 
	                                 kwlist, &getattr_cb, &readlink_cb,
	                                 &readdir_cb, &mknod_cb, &mkdir_cb,
	                                 &unlink_cb, &rmdir_cb, &symlink_cb,
	                                 &rename_cb, &link_cb, &chmod_cb,
	                                 &chown_cb, &truncate_cb, &utime_cb,
	                                 &open_cb, &read_cb, &write_cb,
	                                 &release_cb, &statfs_cb, &fsync_cb,
	                                 &create_cb, &opendir_cb,
	                                 &releasedir_cb, &fsyncdir_cb,
	                                 &flush_cb, &ftruncate_cb,
	                                 &fgetattr_cb, &getxattr_cb,
	                                 &listxattr_cb, &setxattr_cb,
	                                 &removexattr_cb, &access_cb,
	                                 &lock_cb, &utimens_cb, &bmap_cb,
	                                 &fsinit_cb, &fsdestroy_cb,
	                                 &fargseq, &multithreaded))
		return NULL;

#define DO_ONE_ATTR_AS(fname, pyname)		\
	 if(pyname ## _cb) {			\
		Py_INCREF(pyname ## _cb);	\
		op.fname = pyname ## _func;	\
	} else					\
		op.fname = NULL;

#define DO_ONE_ATTR(name)			\
	DO_ONE_ATTR_AS(name, name)

	DO_ONE_ATTR(getattr);
	DO_ONE_ATTR(readlink);
#if FUSE_VERSION >= 23
	DO_ONE_ATTR(opendir);
	DO_ONE_ATTR(releasedir);
	DO_ONE_ATTR(fsyncdir);
	DO_ONE_ATTR(readdir);
#else
	DO_ONE_ATTR_AS(getdir, readdir);
#endif
	DO_ONE_ATTR(mknod);
	DO_ONE_ATTR(mkdir);
	DO_ONE_ATTR(unlink);
	DO_ONE_ATTR(rmdir);
	DO_ONE_ATTR(symlink);
	DO_ONE_ATTR(rename);
	DO_ONE_ATTR(link);
	DO_ONE_ATTR(chmod);
	DO_ONE_ATTR(chown);
	DO_ONE_ATTR(truncate);
	DO_ONE_ATTR(utime);
	DO_ONE_ATTR(open);
	DO_ONE_ATTR(read);
	DO_ONE_ATTR(write);
	DO_ONE_ATTR(release);
	DO_ONE_ATTR(statfs);
	DO_ONE_ATTR(fsync);
	DO_ONE_ATTR(flush);
	DO_ONE_ATTR(getxattr);
	DO_ONE_ATTR(listxattr);
	DO_ONE_ATTR(setxattr);
	DO_ONE_ATTR(removexattr);
#if FUSE_VERSION >= 25
	DO_ONE_ATTR(ftruncate);
	DO_ONE_ATTR(fgetattr);
	DO_ONE_ATTR(access);
	DO_ONE_ATTR(create);
#endif
#if FUSE_VERSION >= 26
	DO_ONE_ATTR(lock);
	DO_ONE_ATTR(utimens);
	DO_ONE_ATTR(bmap);
#endif
#if FUSE_VERSION >= 23
	DO_ONE_ATTR_AS(init, fsinit);
	DO_ONE_ATTR_AS(destroy, fsdestroy);
#endif

#undef DO_ONE_ATTR
#undef DO_ONE_ATTR_AS

	if (!fargseq || !PySequence_Check(fargseq) ||
            (fargc = PySequence_Length(fargseq)) == 0) {
		PyErr_SetString(PyExc_TypeError,
		                "fuse_args is not a non-empty sequence");
		return(NULL);
	}

 	fargv = malloc(fargc * sizeof(char *)); 	
	if (!fargv)
		return(PyErr_NoMemory());

	if (fargseq) {
		for (i=0; i < fargc; i++) {
			PyObject *pa;
	
			pa = PySequence_GetItem(fargseq, i);
			if (!PyString_Check(pa)) {
				Py_DECREF(pa);

				PyErr_SetString(PyExc_TypeError,
			                        "fuse argument is not a string");
		                return(NULL);
			}
			fargv[i] =  PyString_AsString(pa);

			Py_DECREF(pa);
		}
	}

	/*
   	 * We don't use the mthp value, set below. We just pass it on so that
   	 * the lib won't end up in dereferring a NULL pointer.
   	 * (Later versions check for NULL, nevertheless we play safe.)
   	 */
#if FUSE_VERSION >= 26
	fuse = fuse_setup(fargc, fargv, &op, sizeof(op), &fmp, &mthp, NULL);
#elif FUSE_VERSION >= 22
	fuse = fuse_setup(fargc, fargv, &op, sizeof(op), &fmp, &mthp, &fd);
#else
	fuse = __fuse_setup(fargc, fargv, &op, &fmp, &mthp, &fd);
#endif
	free(fargv);

	if (fuse == NULL) {
		PyErr_SetString(Py_FuseError, "filesystem initialization failed");

		return (NULL);
	}

#ifndef WITH_THREAD
	if (multithreaded) {
		multithreaded = 0;
#if PY_MAJOR_VERSION > 2 || (PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION >= 5)
		PyErr_WarnEx(NULL, "Python thread support not available, "
		                   "enforcing single-threaded operation", 1);
#else
		PyErr_Warn(NULL, "Python thread support not available, "
		                 "enforcing single-threaded operation");
#endif
	}
#endif

	if (multithreaded)
		err = pyfuse_loop_mt(fuse);
	else {
		interp = NULL;
		err = fuse_loop(fuse);
	}

#if FUSE_VERSION >= 26
	fuse_teardown(fuse, fmp);	
#elif FUSE_VERSION >= 22
	fuse_teardown(fuse, fd, fmp);
#else
	__fuse_teardown(fuse, fd, fmp);
#endif

	if (err == -1) {
		PyErr_SetString(Py_FuseError, "service loop failed");

		return (NULL);
	}		 

	Py_INCREF(Py_None);
	return Py_None;
}

static char FuseInvalidate__doc__[] =
	"Tell Fuse kernel module to explicitly invalidate a cached inode's contents\n";

static PyObject *
FuseInvalidate(PyObject *self, PyObject *args)
{
	char *path;
	PyObject *ret, *arg1;
	int err;

	if (!(arg1 = PyTuple_GetItem(args, 1)))
		return(NULL);

	if(!PyString_Check(arg1)) {
		PyErr_SetString(PyExc_TypeError, "argument must be a string");

		return(NULL);
	}

	path = PyString_AsString(arg1);

	err = fuse_invalidate(fuse, path);

	ret = PyInt_FromLong(err);

	return(ret);
}

static char FuseGetContext__doc__[] =
	"Return the context of a filesystem operation in a dict. uid, gid, pid\n";

static PyObject *
FuseGetContext(PyObject *self, PyObject *args)
{
	struct fuse_context *fc;
	PyObject *ret;
	PyObject *num;

	fc = fuse_get_context();
	ret = PyDict_New();

	if(!ret)
		return(NULL);

	num = PyInt_FromLong(fc->uid);
	PyDict_SetItemString(ret, "uid", num);	
	Py_XDECREF( num );

	num = PyInt_FromLong(fc->gid);
	PyDict_SetItemString(ret, "gid", num);	
	Py_XDECREF( num );

	num = PyInt_FromLong(fc->pid);
	PyDict_SetItemString(ret, "pid", num);	
	Py_XDECREF( num );

	return(ret);
}

static char FuseAPIVersion__doc__[] =
	"Return FUSE API version.\n";

static PyObject *
FuseAPIVersion(PyObject *self, PyObject *args)
{
	PyObject *favers = PyInt_FromLong(FUSE_VERSION);

	return favers;
}

static PyMethodDef Fuse_methods[] = {
	{"main",	(PyCFunction)Fuse_main,	 METH_VARARGS|METH_KEYWORDS},
	{"FuseGetContext", (PyCFunction)FuseGetContext, METH_VARARGS, FuseGetContext__doc__},
	{"FuseInvalidate", (PyCFunction)FuseInvalidate, METH_VARARGS, FuseInvalidate__doc__},
	{"FuseAPIVersion", (PyCFunction)FuseAPIVersion, METH_NOARGS,  FuseAPIVersion__doc__},
	{NULL,		NULL}		/* sentinel */
};


/* Initialization function for the module (*must* be called init_fuse) */

DL_EXPORT(void)
init_fuse(void)
{
	PyObject *m, *d;
 
	/* Create the module and add the functions */
	m = Py_InitModule("_fuse", Fuse_methods);

	/* Add some symbolic constants to the module */
	d = PyModule_GetDict(m);
	Py_FuseError = PyErr_NewException("fuse.FuseError", NULL, NULL);
	PyDict_SetItemString(d, "FuseError", Py_FuseError);
	/* compat */
	PyDict_SetItemString(d, "error", Py_FuseError);
}
