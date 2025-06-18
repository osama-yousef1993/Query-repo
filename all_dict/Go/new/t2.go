package Gateways

/*
	Supporting Zephr API Documentation
	https://support.zephr.com/admin-api
*/
import (
	"context"
	"errors"
	"io/ioutil"

	"net/http"
	"net/url"
	"strings"

	"github.com/Forbes-Media/forbes-digital-assets/HTTPGateway"
	"github.com/Forbes-Media/forbes-digital-assets/refactored/common/Gateways/structs"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type CordialGateWay interface {
	GetContacts(context.Context, structs.GetCordialContactsParams) (*structs.GetCordialContactsResponse, error)
	//GetAccountLists(context.Context)
}

// Iplements the CordialGateway
type cordialGateWay struct {
	APIKey string
	Url    string
}

func (c *cordialGateWay) GetContacts(context.Context, structs.GetCordialContactsParams) (*structs.GetCordialContactsResponse, error) {
	baseURL, err := url.Parse(c.Url)
	if err != nil {
		goto ERR
	}

	query := baseURL.Query()
	query.Set()

	return nil, nil

ERR:
	return nil, nil
}

/*
CheckZephr  Makes a request to Zephr services and returns an object of the desired generic type.
The Generic type should be passed in a the object you are expecting back from the response.
Returns object of type T (use classes from Zephr.go)
*/

func callCordial[T interface{}](ctx context.Context, host string, reqbody string, httpMethod string, header http.Header) (*T, error) {

	labels := make(map[string]string)
	span := trace.SpanFromContext(ctx)
	defer span.End()

	labels["function"] = "CheckZephr"
	labels["UUID"] = uuid.New().String()
	labels["spanID"] = span.SpanContext().SpanID().String()
	labels["traceID"] = span.SpanContext().TraceID().String()

	span.SetAttributes(attribute.String("UUID", labels["UUID"]))
	span.AddEvent("start CheckZephr")
	var data T
	req, _ := http.NewRequest(httpMethod, host, strings.NewReader(reqbody))
	req.Header = header

	resp := HTTPGateway.Process(req)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	data, err = HTTPGateway.ConvertResponseToObj[T](body, resp.Header["Content-Type"][0])

	resp.Body.Close()

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err

	}
	span.SetStatus(codes.Ok, "CheckZephr")
	return &data, nil

}
