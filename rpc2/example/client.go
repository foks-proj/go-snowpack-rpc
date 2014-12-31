package main

import (
	"fmt"
	"github.com/maxtaco/go-framed-msgpack-rpc/rpc2"
	"net"
)

type GenericClient interface {
	Call(method string, arg interface{}, res interface{}) error
}

//---------------------------------------------------------------------

type ArithClient struct {
	cli GenericClient
}

func (a ArithClient) Add(arg AddArgs) (ret int, err error) {
	err = a.cli.Call("test.1.arith.add", arg, &ret)
	return
}

//---------------------------------------------------------------------

type Client struct {
}

func (s *Client) Run() (err error) {
	var c net.Conn
	fmt.Printf("Listening on port %d...\n", port)
	if c, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err != nil {
		return
	}

	xp := rpc2.NewTransport(c, nil)
	cli := ArithClient{rpc2.NewClient(xp)}

	for A := 10; A < 23; A += 2 {
		var res int
		if res, err = cli.Add(AddArgs{A: A, B: 34}); err != nil {
			return
		}
		fmt.Printf("result is -> %v\n", res)
	}

	return nil
}
