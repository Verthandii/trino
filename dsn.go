package trino

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config is a configuration that can be encoded to a DSN string.
type Config struct {
	ServerURI          string            // URI of the Trino server, e.g. http://user@localhost:8080
	Source             string            // Source of the connection (optional)
	Catalog            string            // Catalog (optional)
	Schema             string            // Schema (optional)
	SessionProperties  map[string]string // Session properties (optional)
	CustomClientName   string            // Custom client name (optional)
	KerberosEnabled    string            // KerberosEnabled (optional, default is false)
	KerberosKeytabPath string            // Kerberos Keytab Path (optional)
	KerberosPrincipal  string            // Kerberos Principal used to authenticate to KDC (optional)
	KerberosRealm      string            // The Kerberos Realm (optional)
	KerberosConfigPath string            // The krb5 config path (optional)
	SSLCertPath        string            // The SSL cert path for TLS verification (optional)
}

// FormatDSN returns a DSN string from the configuration.
func (c *Config) FormatDSN() (string, error) {
	serverURL, err := url.Parse(c.ServerURI)
	if err != nil {
		return "", err
	}
	var sessionkv []string
	if c.SessionProperties != nil {
		for k, v := range c.SessionProperties {
			sessionkv = append(sessionkv, k+"="+v)
		}
	}
	source := c.Source
	if source == "" {
		source = "trino-go-client"
	}
	query := make(url.Values)
	query.Add("source", source)

	KerberosEnabled, _ := strconv.ParseBool(c.KerberosEnabled)
	isSSL := serverURL.Scheme == "https"

	if isSSL && c.SSLCertPath != "" {
		query.Add(SSLCertPathConfig, c.SSLCertPath)
	}

	if KerberosEnabled {
		query.Add(KerberosEnabledConfig, "true")
		query.Add(_kerberosKeytabPathConfig, c.KerberosKeytabPath)
		query.Add(_kerberosPrincipalConfig, c.KerberosPrincipal)
		query.Add(_kerberosRealmConfig, c.KerberosRealm)
		query.Add(_kerberosConfigPathConfig, c.KerberosConfigPath)
		if !isSSL {
			return "", fmt.Errorf("trino: client configuration error, SSL must be enabled for secure env")
		}
	}

	for k, v := range map[string]string{
		"catalog":            c.Catalog,
		"schema":             c.Schema,
		"session_properties": strings.Join(sessionkv, ","),
		"custom_client":      c.CustomClientName,
	} {
		if v != "" {
			query[k] = []string{v}
		}
	}
	serverURL.RawQuery = query.Encode()
	return serverURL.String(), nil
}
