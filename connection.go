package trino

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"gopkg.in/jcmturner/gokrb5.v6/client"
	"gopkg.in/jcmturner/gokrb5.v6/config"
	"gopkg.in/jcmturner/gokrb5.v6/keytab"
)

// Conn is a Trino connection. implements driver.Conn & driver.ConnPrepareContext
type Conn struct {
	baseURL         string
	auth            *url.Userinfo
	httpClient      http.Client
	httpHeaders     http.Header
	kerberosClient  client.Client
	kerberosEnabled bool
}

var (
	_ driver.Conn               = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
)

func newConn(dsn string) (*Conn, error) {
	serverURL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("trino: malformed dsn: %v", err)
	}

	query := serverURL.Query()

	kerberosEnabled, _ := strconv.ParseBool(query.Get(KerberosEnabledConfig))

	var kerberosClient client.Client

	if kerberosEnabled {
		kt, err := keytab.Load(query.Get(_kerberosKeytabPathConfig))
		if err != nil {
			return nil, fmt.Errorf("trino: Error loading Keytab: %v", err)
		}

		kerberosClient = client.NewClientWithKeytab(query.Get(_kerberosPrincipalConfig), query.Get(_kerberosRealmConfig), kt)
		conf, err := config.Load(query.Get(_kerberosConfigPathConfig))
		if err != nil {
			return nil, fmt.Errorf("trino: Error loading krb config: %v", err)
		}

		kerberosClient.WithConfig(conf)

		loginErr := kerberosClient.Login()
		if loginErr != nil {
			return nil, fmt.Errorf("trino: Error login to KDC: %v", loginErr)
		}
	}

	var httpClient = http.DefaultClient
	if clientKey := query.Get("custom_client"); clientKey != "" {
		httpClient = getCustomClient(clientKey)
		if httpClient == nil {
			return nil, fmt.Errorf("trino: custom client not registered: %q", clientKey)
		}
	} else if certPath := query.Get(SSLCertPathConfig); certPath != "" && serverURL.Scheme == "https" {
		cert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, fmt.Errorf("trino: Error loading SSL Cert File: %v", err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)

		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: certPool,
				},
			},
		}
	}

	c := &Conn{
		baseURL:         serverURL.Scheme + "://" + serverURL.Host,
		httpClient:      *httpClient,
		httpHeaders:     make(http.Header),
		kerberosClient:  kerberosClient,
		kerberosEnabled: kerberosEnabled,
	}

	var user string
	if serverURL.User != nil {
		user = serverURL.User.Username()
		pass, _ := serverURL.User.Password()
		if pass != "" && serverURL.Scheme == "https" {
			c.auth = serverURL.User
		}
	}

	for k, v := range map[string]string{
		XTrinoUserHeader:     user,
		_xTrinoSourceHeader:  query.Get("source"),
		_xTrinoCatalogHeader: query.Get("catalog"),
		_xTrinoSchemaHeader:  query.Get("schema"),
		_xTrinoSessionHeader: query.Get("session_properties"),
	} {
		if v != "" {
			c.httpHeaders.Add(k, v)
		}
	}

	return c, nil
}

// Begin implements the driver.Conn interface.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrOperationNotSupported
}

// Prepare implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

// PrepareContext implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &driverStmt{conn: c, query: query}, nil
}

// Close implements the driver.Conn interface.
func (c *Conn) Close() error {
	return nil
}

// ResetSession implements driver.SessionResetter
func (c *Conn) ResetSession(ctx context.Context) error {
	return nil
}

func (c *Conn) newRequest(method, url string, body io.Reader, hs http.Header) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("trino: %v", err)
	}

	if c.kerberosEnabled {
		err = c.kerberosClient.SetSPNEGOHeader(req, "presto/"+req.URL.Hostname())
		if err != nil {
			return nil, fmt.Errorf("error setting client SPNEGO header: %v", err)
		}
	}

	for k, v := range c.httpHeaders {
		req.Header[k] = v
	}
	for k, v := range hs {
		req.Header[k] = v
	}

	if c.auth != nil {
		pass, _ := c.auth.Password()
		req.SetBasicAuth(c.auth.Username(), pass)
	}
	return req, nil
}

func (c *Conn) roundTrip(ctx context.Context, req *http.Request) (*http.Response, error) {
	delay := 100 * time.Millisecond
	const maxDelayBetweenRequests = float64(15 * time.Second)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			timeout := DefaultQueryTimeout
			if deadline, ok := ctx.Deadline(); ok {
				timeout = deadline.Sub(time.Now())
			}
			client := c.httpClient
			client.Timeout = timeout
			resp, err := client.Do(req)
			if err != nil {
				return nil, &ErrQueryFailed{Reason: err}
			}
			switch resp.StatusCode {
			case http.StatusOK:
				return resp, nil
			case http.StatusServiceUnavailable:
				resp.Body.Close()
				timer.Reset(delay)
				delay = time.Duration(math.Min(
					float64(delay)*math.Phi,
					maxDelayBetweenRequests,
				))
				continue
			default:
				return nil, newErrQueryFailedFromResponse(resp)
			}
		}
	}
}
