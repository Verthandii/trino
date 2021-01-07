package trino

type version string

const (
	_trinoVersion  version = "Trino"
	_prestoVersion version = "Presto"
)

var (
	v = _trinoVersion
)

// VersionTrion 设置当前 header 头为 trino 所需的
func VersionTrino() {
	v = _trinoVersion
}

// VersionPresto 设置当前 header 头为 persto 所需的
func VersionPresto() {
	v = _prestoVersion
}
