package kerrors

// Коды ошибок ядра Linux
const (
	EPERM     int64 = 1  // Operation not permitted
	ENOENT    int64 = 2  // No such file or directory
	ENOMEM    int64 = 12 // Out of memory
	EEXIST    int64 = 17 // File exists
	ENOTDIR   int64 = 20 // Not a directory
	EISDIR    int64 = 21 // Is a directory
	EINVAL    int64 = 22 // Invalid argument
	ENOTEMPTY int64 = 39 // Directory not empty

	ENOMEM_NEG int64 = -ENOMEM // Out of memory (negative)
	EINVAL_NEG int64 = -EINVAL // Invalid argument (negative)
)
