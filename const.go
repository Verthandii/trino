package trino

const (
	_preparedStatementHeader  = "X-Presto-Prepared-Statement"
	_preparedStatementName    = "_trino_go"
	_trinoUserHeader          = "X-Presto-User"
	_trinoSourceHeader        = "X-Presto-Source"
	_trinoCatalogHeader       = "X-Presto-Catalog"
	_trinoSchemaHeader        = "X-Presto-Schema"
	_trinoSessionHeader       = "X-Presto-Session"
	_trinoQueryCallbackHeader = "X-Presto-Callback"

	KerberosEnabledConfig    = "KerberosEnabled"
	kerberosKeytabPathConfig = "KerberosKeytabPath"
	kerberosPrincipalConfig  = "KerberosPrincipal"
	kerberosRealmConfig      = "KerberosRealm"
	kerberosConfigPathConfig = "KerberosConfigPath"
	SSLCertPathConfig        = "SSLCertPath"
)
