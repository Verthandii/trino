package trino

const (
	_preparedStatementHeader = "X-Presto-Prepared-Statement"
	_preparedStatementName   = "_trino_go"

	XTrinoUserHeader     = "X-Trino-User"
	_xTrinoSourceHeader  = "X-Trino-Source"
	_xTrinoCatalogHeader = "X-Trino-Catalog"
	_xTrinoSchemaHeader  = "X-Trino-Schema"
	_xTrinoSessionHeader = "X-Trino-Session"

	XPrestoUserHeader     = "X-Presto-User"
	_xPrestoSourceHeader  = "X-Presto-Source"
	_xPrestoCatalogHeader = "X-Presto-Catalog"
	_xPrestoSchemaHeader  = "X-Presto-Schema"
	_xPrestoSessionHeader = "X-Presto-Session"

	CallbackHeader = "Callback"

	KerberosEnabledConfig     = "KerberosEnabled"
	_kerberosKeytabPathConfig = "KerberosKeytabPath"
	_kerberosPrincipalConfig  = "KerberosPrincipal"
	_kerberosRealmConfig      = "KerberosRealm"
	_kerberosConfigPathConfig = "KerberosConfigPath"
	SSLCertPathConfig         = "SSLCertPath"
)

var (
	vhs = map[version]map[string]string{
		_trinoVersion: {
			"user":    XTrinoUserHeader,
			"source":  _xTrinoSourceHeader,
			"catalog": _xTrinoCatalogHeader,
			"schema":  _xTrinoSchemaHeader,
			"session": _xTrinoSessionHeader,
		},
		_prestoVersion: {
			"user":    XPrestoUserHeader,
			"source":  _xPrestoSourceHeader,
			"catalog": _xPrestoCatalogHeader,
			"schema":  _xPrestoSchemaHeader,
			"session": _xPrestoSessionHeader,
		},
	}
)
