package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

type testConnectionHandler struct{}

var _ ConnectionHandler = testConnectionHandler{}

func (testConnectionHandler) OnConnect(context.Context, *Connection, GenericClient, *Server) error {
	return nil
}

func (testConnectionHandler) OnConnectError(_ error, _ time.Duration) {
}

func (testConnectionHandler) OnDoCommandError(_ error, _ time.Duration) {
}

func (testConnectionHandler) OnDisconnected(_ context.Context, _ DisconnectStatus) {
}

func (testConnectionHandler) ShouldRetry(_ Methoder, _ error) bool {
	return false
}

func (testConnectionHandler) ShouldRetryOnConnect(_ error) bool {
	return false
}

func (testConnectionHandler) HandlerName() string {
	return "testConnectionHandler"
}

type singleTransport struct {
	t Transporter
}

var _ ConnectionTransport = singleTransport{}

// Dial is an implementation of the ConnectionTransport interface.
func (st singleTransport) Dial(_ context.Context) (Transporter, error) {
	if !st.t.IsConnected() {
		return nil, io.EOF
	}
	return st.t, nil
}

// IsConnected is an implementation of the ConnectionTransport interface.
func (st singleTransport) IsConnected() bool {
	return st.t.IsConnected()
}

// Finalize is an implementation of the ConnectionTransport interface.
func (st singleTransport) Finalize() {}

// Close is an implementation of the ConnectionTransport interface.
func (st singleTransport) Close() {}

type testStatus struct {
	Code int
}

func testWrapError(_ error) interface{} {
	return &testStatus{}
}

func testLogTags(_ context.Context) (map[interface{}]string, bool) {
	return nil, false
}

type throttleError struct {
	Err error
}

func (e throttleError) ToStatus() (s testStatus) {
	s.Code = 15
	return
}

func (e throttleError) Error() string {
	return e.Err.Error()
}

type testErrorUnwrapper struct{}

var _ ErrorUnwrapper = testErrorUnwrapper{}

func (eu testErrorUnwrapper) Timeout() time.Duration {
	return 0
}

func (eu testErrorUnwrapper) MakeArg() interface{} {
	return &testStatus{}
}

func (eu testErrorUnwrapper) UnwrapError(arg interface{}) (appError error, dispatchError error) {
	s, ok := arg.(*testStatus)
	if !ok {
		return nil, errors.New("Error converting arg to testStatus object")
	}
	if s == nil || s.Code == 0 {
		return nil, nil
	}

	switch s.Code {
	case 15:
		appError = throttleError{errors.New("throttle")}
	default:
		panic("Unknown testing error")
	}
	return appError, nil
}

// TestLogger is an interface for things, like *testing.T, that have a
// Logf and Helper function.
type TestLogger interface {
	Logf(format string, args ...interface{})
	Helper()
}

const testMaxFrameLength = 1024

// MakeConnectionForTest returns a Connection object, and a net.Conn
// object representing the other end of that connection.
func MakeConnectionForTest(t TestLogger) (net.Conn, *Connection) {
	clientConn, serverConn := net.Pipe()
	logOutput := testLogOutput{t: t}
	logFactory := NewSimpleLogFactory(&logOutput, nil)
	instrumenterStorage := NewMemoryInstrumentationStorage()
	transporter := NewTransport(context.TODO(), clientConn, logFactory,
		instrumenterStorage, testWrapError, testMaxFrameLength)
	st := singleTransport{transporter}
	opts := ConnectionOpts{
		WrapErrorFunc: testWrapError,
		TagsFunc:      testLogTags,
	}
	conn := NewConnectionWithTransport(testConnectionHandler{}, st,
		testErrorUnwrapper{}, &logOutput, opts)
	return serverConn, conn
}
