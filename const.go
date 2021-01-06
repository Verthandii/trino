package trino

const (
	_preparedStatementHeader = "X-Presto-Prepared-Statement"
	_preparedStatementName   = "_trino_go"
	XTrinoUserHeader         = "X-Trino-User"
	_xTrinoSourceHeader      = "X-Trino-Source"
	_xTrinoCatalogHeader     = "X-Trino-Catalog"
	_xTrinoSchemaHeader      = "X-Trino-Schema"
	_xTrinoSessionHeader     = "X-Trino-Session"
	XTrinoCallbackHeader     = "X-Trino-Callback"

	KerberosEnabledConfig     = "KerberosEnabled"
	_kerberosKeytabPathConfig = "KerberosKeytabPath"
	_kerberosPrincipalConfig  = "KerberosPrincipal"
	_kerberosRealmConfig      = "KerberosRealm"
	_kerberosConfigPathConfig = "KerberosConfigPath"
	SSLCertPathConfig         = "SSLCertPath"
)
