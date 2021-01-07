package trino

type version string

var (
	v version = "Trino"
)

const (
	_trinoVersion  version = "Trino"
	_prestoVersion version = "Presto"
)

func VersionTrino() {
	v = _trinoVersion
}

func VersionPresto() {
	v = _prestoVersion
}
