package rpc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/internal/testcapnp"
	"capnproto.org/go/capnp/v3/rpc/transport"
	rpccp "capnproto.org/go/capnp/v3/std/capnp/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSenderPromiseFulfill(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[testcapnp.PingPong]()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p),
	})
	defer finishTest(t, conn, p2)

	// 1. Send bootstrap.
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}
	// 2. Receive return.
	var bootExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		bootExportID = desc.SenderPromise
	}
	// 3. Fulfill promise
	{
		pp := testcapnp.PingPong_ServerToClient(&pingPonger{})
		defer pp.Release()
		r.Fulfill(pp)
	}
	// 4. Receive resolve.
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, bootExportID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		assert.NotEqual(t, bootExportID, desc.SenderHosted)
	}
}

// Tests that if we get an unimplemented message in response to a resolve message, we correctly
// drop the capability.
func TestResolveUnimplementedDrop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[testcapnp.Empty]()

	provider := testcapnp.EmptyProvider_ServerToClient(emptyShutdownerProvider{
		result: p,
	})

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(provider),
	})
	defer finishTest(t, conn, p2)

	// 1. Send bootstrap.
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}
	// 2. Receive return.
	var bootExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		bootExportID = desc.SenderHosted
	}
	onShutdown := make(chan struct{})
	// 3. Send finish
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_finish,
			Finish: &rpcFinish{
				QuestionID:        0,
				ReleaseResultCaps: false,
			},
		}))
	}
	// 4. Call getEmpty
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_call,
			Call: &rpcCall{
				QuestionID: 1,
				Target: rpcMessageTarget{
					Which:       rpccp.MessageTarget_Which_importedCap,
					ImportedCap: bootExportID,
				},
				InterfaceID: testcapnp.EmptyProvider_TypeID,
				MethodID:    0,
				Params:      rpcPayload{},
			},
		}))
	}
	// 5. Receive return.
	var emptyExportID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, uint32(1), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Nil(t, rmsg.Return.Exception)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		emptyExportID = desc.SenderPromise
	}
	// 6. Fulfill promise
	{
		pp := testcapnp.Empty_ServerToClient(emptyShutdowner{
			onShutdown: onShutdown,
		})
		r.Fulfill(pp)
		pp.Release()
	}
	// 7. Receive resolve, send unimplemented
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, emptyExportID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_senderHosted, desc.Which)
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which:         rpccp.Message_Which_unimplemented,
			Unimplemented: rmsg,
		}))
	}
	// 8. Drop the promise on our side. Otherwise it will stay alive because of
	// the bootstrap interface:
	{
		p.Release()
	}
	// 9. Send finish
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_finish,
			Finish: &rpcFinish{
				QuestionID:        1,
				ReleaseResultCaps: true,
			},
		}))
	}
	<-onShutdown // Will hang unless the capability is dropped
}

type emptyShutdownerProvider struct {
	result testcapnp.Empty
}

func (e emptyShutdownerProvider) GetEmpty(ctx context.Context, p testcapnp.EmptyProvider_getEmpty) error {
	results, err := p.AllocResults()
	if err != nil {
		return err
	}
	results.SetEmpty(e.result)
	return nil
}

type emptyShutdowner struct {
	onShutdown chan<- struct{} // closed on shutdown
}

func (s emptyShutdowner) Shutdown() {
	close(s.onShutdown)
}

// Tests fulfilling a senderPromise with something hosted on the receiver
func TestDisembargoSenderPromise(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[capnp.Client]()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p), // The bootstrap intf of conn/p1 is promise `p`
		RemotePeerID:    rpc.PeerID{Value: "p1"},
	})
	defer finishTest(t, conn, p2)

	// P2 asks for P1's bootstrap interface (send Bootstrap from P2 to P1).
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}

	// Receive return from P1. `theirBootstrapID` will point to a P1
	// promise to return the bootstrap interface (i.e. a promise to return
	// once `p` resolves).
	var theirBootstrapID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()

		// Assert that it contains a result and that the result is
		// index 0 in the cap table and that the result is a promise.
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		theirBootstrapID = desc.SenderPromise
	}

	// For conveience, we use the other peer's (i.e. P2's) bootstrap
	// interface as the thing to resolve to.
	//
	// bsClient is a promise (in P1) that will resolve to P2's bootstrap
	// interface.
	bsClient := conn.Bootstrap(ctx)
	defer bsClient.Release()

	// P2 receives the bootstrap request, sends return.
	myBootstrapID := uint32(12) // Any dummy value. This is the cap in P2.
	var incomingBSQid uint32
	{
		// Assert the next message is a bootstrap request.
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_bootstrap, rmsg.Which)
		incomingBSQid = rmsg.Bootstrap.QuestionID

		outMsg, err := p2.NewMessage()
		assert.NoError(t, err)
		iface := capnp.NewInterface(outMsg.Message().Segment(), 0)

		// Reply with the return, pointing out that P2's bootstrap
		// interface is located at the `myBootstrapID` entry in P2's
		// export table.
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_return,
			Return: &rpcReturn{
				AnswerID: incomingBSQid,
				Which:    rpccp.Return_Which_results,
				Results: &rpcPayload{
					Content: iface.ToPtr(),
					CapTable: []rpcCapDescriptor{
						{
							Which:        rpccp.CapDescriptor_Which_senderHosted,
							SenderHosted: myBootstrapID,
						},
					},
				},
			},
		}))
	}

	// P1 accepts P2's return and resolves the bootstrap interface. It
	// sends a Finish to flag that it has no more pipelined calls into the
	// original Bootstrap() call and P2 may release resources.
	assert.NoError(t, bsClient.Resolve(ctx))

	// P2 receives finish.
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_finish, rmsg.Which)
		assert.Equal(t, incomingBSQid, rmsg.Finish.QuestionID)
	}

	// Resolve P1's bootstrap capability as P2's bootstrap capability.
	// Semantically, this means P1 proxies calls to P2.
	r.Fulfill(bsClient)

	// r.Fulfill() causes all references to P1's bootstrap capability (which
	// was initialized as the `p` promise) to be completed. This triggers
	// a Resolve message from P1 to P2 (recall that the very first message
	// in this test was P2 asking for P1's bootstrap interface).
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, theirBootstrapID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_receiverHosted, desc.Which)

		// Recall that we resolved P1's bootstrap promise with P2's
		// bootstrap capability (a concrete exported capability, at
		// `myBootstrapID`).
		assert.Equal(t, myBootstrapID, desc.ReceiverHosted)
	}

	// The prior message resolved P1's bootstrap interface (promise `p`,
	// identified in P1's export table by `theirBootstrapID`) into P2's
	// bootstrap interface (the concrete capability `myBootstrapID`).
	//
	// P2 detects that is a local capability (i.e. looped back), thus
	// it needs to send a disembargo so that P1 will start resolving
	// any pipelined calls (if there are any).

	// P2 sends the disembargo.
	embargoID := uint32(7) // Any dummy value.
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_disembargo,
			Disembargo: &rpcDisembargo{
				Context: rpcDisembargoContext{
					Which: rpccp.Disembargo_context_Which_senderLoopback,

					// The original promise resolved to a
					// capability in P2 (who is sending this
					// very message, thus SenderLoopback).
					SenderLoopback: embargoID,
				},
				Target: rpcMessageTarget{
					Which: rpccp.MessageTarget_Which_importedCap,

					// P2 is disembargo'ing the original
					// promise it received from P1.
					ImportedCap: theirBootstrapID,
				},
			},
		}))
	}

	// P1 sends any previously-embargoed and now disembargoed pipelined
	// calls. This test has none.

	// P1 echoes back the disembargo to P2 to let it know it has finished
	// processing pipelined calls.

	// P2 Receives disembargo:
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_disembargo, rmsg.Which)
		d := rmsg.Disembargo

		// Echo of SenderLoopback in ReceiverLoopback
		assert.Equal(t, rpccp.Disembargo_context_Which_receiverLoopback, d.Context.Which)
		assert.Equal(t, embargoID, d.Context.ReceiverLoopback)
		tgt := d.Target
		assert.Equal(t, rpccp.MessageTarget_Which_importedCap, tgt.Which)

		// P2 exported the concrete capability in `myBootstrapID`. P1
		// now knows this and is replying that info back.
		assert.Equal(t, myBootstrapID, tgt.ImportedCap)
	}
}

// TestDisembargoSenderPromiseWithPipeline tests disembargoing a sender promise
// when there are pipelined calls.
func TestDisembargoSenderPromiseWithPipeline(t *testing.T) {
	t.Parallel()

	const concreteBootstrapInterfaceID uint64 = 0xbaba001337
	const methodIdOrd uint16 = 0x0f30

	ctx := context.Background()
	p, r := capnp.NewLocalPromise[capnp.Client]()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	conn := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p), // The bootstrap intf of conn/p1 is promise `p`
		RemotePeerID:    rpc.PeerID{Value: "p1"},
	})
	defer finishTest(t, conn, p2)

	// P2 asks for P1's bootstrap interface (send Bootstrap from P2 to P1).
	{
		msg := &rpcMessage{
			Which:     rpccp.Message_Which_bootstrap,
			Bootstrap: &rpcBootstrap{QuestionID: 0},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}

	// Receive return from P1. `theirBootstrapID` will point to a P1
	// promise to return the bootstrap interface (i.e. a promise to return
	// once `p` resolves).
	var theirBootstrapID uint32
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()

		// Assert that it contains a result and that the result is
		// index 0 in the cap table and that the result is a promise.
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, uint32(0), rmsg.Return.AnswerID)
		assert.Equal(t, rpccp.Return_Which_results, rmsg.Return.Which)
		assert.Equal(t, 1, len(rmsg.Return.Results.CapTable))
		desc := rmsg.Return.Results.CapTable[0]
		assert.Equal(t, rpccp.CapDescriptor_Which_senderPromise, desc.Which)
		theirBootstrapID = desc.SenderPromise
	}

	fmt.Println("p2 received bootstrap")
	time.Sleep(time.Second)
	fmt.Println("gonna send call")

	// P2 makes a pipelined call on that returned promise. This will get
	// embargoed in P1. Note this is a call on the bootstrap interface, thus
	// must be supported by level 0 implementations.
	p2Call01Id := uint32(0xaabbccdd)
	{
		msg := &rpcMessage{
			Which: rpccp.Message_Which_call,
			Call: &rpcCall{
				QuestionID: p2Call01Id,
				Target: rpcMessageTarget{
					Which: rpccp.MessageTarget_Which_promisedAnswer,
					PromisedAnswer: &rpcPromisedAnswer{
						// ???
						//
						// Doc says "ID of the
						// question (in the sender's
						// question table / receiver's
						// answer table)"
						//
						// But I think this is backwards.
						QuestionID: theirBootstrapID,
					},
				},
				InterfaceID: concreteBootstrapInterfaceID,
				MethodID:    methodIdOrd,
				SendResultsTo: rpcCallSendResultsTo{
					Which: rpccp.Call_sendResultsTo_Which_caller,
				},
			},
		}
		assert.NoError(t, sendMessage(ctx, p2, msg))
	}

	// The previous call is now pending in P1. It's waiting for P1's own
	// bootstrap interface promise (i.e. `p`) to be resolved before it
	// can continue.

	fmt.Println("sent call")
	time.Sleep(time.Second)
	fmt.Println("gonna ask for P2 bootstrap")

	// For conveience, we use the other peer's (i.e. P2's) bootstrap
	// interface as the thing to resolve to.
	//
	// bsClient is a promise (in P1) that will resolve to P2's bootstrap
	// interface.
	//
	// conn.Bootstrap() asks for P2's bootstrap interface.
	bsClient := conn.Bootstrap(ctx)
	defer bsClient.Release()

	// P2 receives the bootstrap request, sends return.
	myBootstrapID := uint32(12) // Any dummy value. This is the cap in P2.
	var incomingBSQid uint32
	{
		// Assert the next message is a bootstrap request.
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_bootstrap, rmsg.Which)
		incomingBSQid = rmsg.Bootstrap.QuestionID

		outMsg, err := p2.NewMessage()
		assert.NoError(t, err)
		iface := capnp.NewInterface(outMsg.Message().Segment(), 0)

		// Reply with the return, pointing out that P2's bootstrap
		// interface is located at the `myBootstrapID` entry in P2's
		// export table.
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_return,
			Return: &rpcReturn{
				AnswerID: incomingBSQid,
				Which:    rpccp.Return_Which_results,
				Results: &rpcPayload{
					Content: iface.ToPtr(),
					CapTable: []rpcCapDescriptor{
						{
							Which:        rpccp.CapDescriptor_Which_senderHosted,
							SenderHosted: myBootstrapID,
						},
					},
				},
			},
		}))
	}

	// P1 accepts P2's return and resolves the bootstrap interface. It
	// sends a Finish to flag that it has no more pipelined calls into the
	// prior conn.Bootstrap() call and P2 may release resources.
	assert.NoError(t, bsClient.Resolve(ctx))

	// P2 receives finish.
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_finish, rmsg.Which)
		assert.Equal(t, incomingBSQid, rmsg.Finish.QuestionID)
	}

	time.Sleep(time.Second)
	fmt.Println("gonna fulfill")
	// time.Sleep(time.Second)

	//stacktrace := make([]byte, 8192)
	//length := runtime.Stack(stacktrace, true)
	//fmt.Println(string(stacktrace[:length]))

	time.Sleep(time.Second)

	// Resolve P1's bootstrap capability as P2's bootstrap capability.
	// Semantically, this means P1 proxies calls to P2.
	//
	// At this point in time, bsClient is already resolved.
	r.Fulfill(bsClient)

	fmt.Println("fulfilled")
	time.Sleep(time.Second)
	fmt.Println("gonna wait for reply")

	// At this point in time, P1 has a fulfilled promise on its bootstrap
	// interface. This was the dependency needed to answer P2's initial
	// query for P1's bootstrap interface.
	//
	// P1 also has a pipelined call (sent by P2) into that promise (that
	// effectively should be proxied by P1 back to P2). The question
	// becomes: what comes first? The pipelined call or the resolve?
	//
	// Due to the nature of this test (and the use of NewLocalPromise), the
	// prior `r.Fulfill()` directly triggers the pending pipelined call
	// _before_ the resolve has a chance to be sent, so that's what we
	// assert next.
	var p1ToP2CallQuestionId uint32
	{
		// P1 sends the pipelined call back to P2.
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_call, rmsg.Which)
		// FIXME: what else to assert?
		assert.Equal(t, methodIdOrd, rmsg.Call.MethodID)
		assert.Equal(t, concreteBootstrapInterfaceID, rmsg.Call.InterfaceID)
		p1ToP2CallQuestionId = rmsg.Call.QuestionID

		fmt.Println("Got call")

	}

	// r.Fulfill() causes all references to P1's bootstrap capability (which
	// was initialized as the `p` promise) to be completed. This triggers
	// a Resolve message from P1 to P2 (recall that the very first message
	// in this test was P2 asking for P1's bootstrap interface).
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_resolve, rmsg.Which)
		assert.Equal(t, theirBootstrapID, rmsg.Resolve.PromiseID)
		assert.Equal(t, rpccp.Resolve_Which_cap, rmsg.Resolve.Which)
		desc := rmsg.Resolve.Cap
		assert.Equal(t, rpccp.CapDescriptor_Which_receiverHosted, desc.Which)

		// Recall that we resolved P1's bootstrap promise with P2's
		// bootstrap capability (a concrete exported capability, at
		// `myBootstrapID`).
		assert.Equal(t, myBootstrapID, desc.ReceiverHosted)
	}
	fmt.Println("Got resolve")

	// Send reply to the P1->P2 call. We do this here instead of immediately
	// after receiving the call in order to process the resolve message first
	// and ensure the finish P2 will receive for this comes later.
	{
		// P2 sends the reply.
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_return,
			Return: &rpcReturn{
				AnswerID: p1ToP2CallQuestionId,
				Which:    rpccp.Return_Which_results,
				Results: &rpcPayload{
					Content: capnp.Ptr{}, // Returning void.
				},
			},
		}))

		fmt.Println("P2 Sent reply to call")

		// P2 Receives the finish.
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_finish, rmsg.Which)

		fmt.Println("P2 received finish")
		time.Sleep(3 * time.Second)
	}

	// The prior message resolved P1's bootstrap interface (promise `p`,
	// identified in P1's export table by `theirBootstrapID`) into P2's
	// bootstrap interface (the concrete capability `myBootstrapID`).
	//
	// P2 detects that is a local capability (i.e. looped back), thus
	// it needs to send a disembargo so that P1 will start resolving
	// any pipelined calls (if there are any).

	// P2 sends the disembargo.
	embargoID := uint32(7) // Any dummy value.
	{
		assert.NoError(t, sendMessage(ctx, p2, &rpcMessage{
			Which: rpccp.Message_Which_disembargo,
			Disembargo: &rpcDisembargo{
				Context: rpcDisembargoContext{
					Which: rpccp.Disembargo_context_Which_senderLoopback,

					// The original promise resolved to a
					// capability in P2 (who is sending this
					// very message, thus SenderLoopback).
					SenderLoopback: embargoID,
				},
				Target: rpcMessageTarget{
					Which: rpccp.MessageTarget_Which_importedCap,

					// P2 is disembargo'ing the original
					// promise it received from P1.
					ImportedCap: theirBootstrapID,
				},
			},
		}))
	}

	fmt.Println("sent disembargo")

	// P1 sends any previously-embargoed and now disembargoed pipelined
	// calls. This test has one embargoed call that can now be returned
	// to P1 (this is the original P2->P1 pipelined call).
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_return, rmsg.Which)
		assert.Equal(t, rmsg.Return.AnswerID, p2Call01Id)
		// FIXME: Anything else to assert?
	}

	// P1 echoes back the disembargo to P2 to let it know it has finished
	// processing pipelined calls.

	// P2 Receives disembargo:
	{
		rmsg, release, err := recvMessage(ctx, p2)
		assert.NoError(t, err)
		defer release()
		assert.Equal(t, rpccp.Message_Which_disembargo, rmsg.Which)
		d := rmsg.Disembargo

		// Echo of SenderLoopback in ReceiverLoopback
		assert.Equal(t, rpccp.Disembargo_context_Which_receiverLoopback, d.Context.Which)
		assert.Equal(t, embargoID, d.Context.ReceiverLoopback)
		tgt := d.Target
		assert.Equal(t, rpccp.MessageTarget_Which_importedCap, tgt.Which)

		// P2 exported the concrete capability in `myBootstrapID`. P1
		// now knows this and is replying that info back.
		assert.Equal(t, myBootstrapID, tgt.ImportedCap)
	}

	fmt.Println("received disembargo")
}

// TestShortensPathAfterResolve verifies that a conn collapses the path to a
// capability if the remote promise resolves to the local vat.
func TestShortensPathAfterResolve(t *testing.T) {
	t.Parallel()

	t.Cleanup(func() { time.Sleep(100 * time.Millisecond) }) // Avoid log error after fail

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	// Promise that will be the bootstrap capability in C1 and will be
	// eventually resolved as the bootstrap capability in C2.
	p, r := capnp.NewLocalPromise[testcapnp.PingPong]()
	defer p.Release()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	// C1 will be proxying calls to C2 (the bootstrap capability is the
	// promise).
	c1 := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p),
		RemotePeerID:    rpc.PeerID{Value: "c1"}, // (really, c2)
		AbortTimeout:    15 * time.Second,
	})

	// C2 is the "echo server".
	ord := &echoNumOrderChecker{t: t}
	c2 := rpc.NewConn(p2, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(testcapnp.PingPong_ServerToClient(ord)),
		RemotePeerID:    rpc.PeerID{Value: "c2"}, // (really, c1)
		AbortTimeout:    15 * time.Second,
	})

	// Request C1's bootstrap interface. This returns a promise that will
	// take a while to resolve, because in C1 this is _also_ a promise back
	// to C2 (in C1 this is `p`, which will be resolved by `r.Fulfill`).
	time.Sleep(time.Second)
	fmt.Println("gonna create c2PromiseToC1")
	c2PromiseToC1 := testcapnp.PingPong(c2.Bootstrap(ctx))
	defer c2PromiseToC1.Release()

	time.Sleep(1000 * time.Millisecond)
	fmt.Println("created second promise")

	// Fetch C2's bootstrap interface (the concrete echo server).
	c1PromiseToC2 := testcapnp.PingPong(c1.Bootstrap(ctx))
	defer c1PromiseToC2.Release()
	require.NoError(t, c1PromiseToC2.Resolve(ctx))

	time.Sleep(1000 * time.Millisecond)
	fmt.Println("resolved c1PromiseToC2")

	// Perform a call in C1 using C2's bootstrap interface (which is
	// already resolved).
	const c1CallValue int64 = 0xc1000042
	c1Call, c1CallRel := echoNum(ctx, c1PromiseToC2, c1CallValue)
	defer c1CallRel()
	c1CallRes, err := c1Call.Struct()
	require.NoError(t, err)
	require.Equal(t, c1CallValue, c1CallRes.N())
	fmt.Println("Got reply")
	time.Sleep(1000 * time.Millisecond)

	go func() {
		fmt.Println("gonna resolve c2PromiseToC1")
		require.NoError(t, c2PromiseToC1.Resolve(ctx))
		fmt.Println("Resolved second promise")
	}()
	time.Sleep(time.Second)

	// Fulfill C1's bootstrap capability with C2's bootstrap capability
	// (i.e. echo server). This resolves the prior c2PromiseToC1 above and
	// shortens the path, because C1's bootstrap resolves to C2's bootstrap.
	fmt.Println("LLL gonna fulfill")
	r.Fulfill(c1PromiseToC2)
	fmt.Println("fulfilled")
	time.Sleep(1000 * time.Millisecond)

	// fmt.Println("gonna resolve c2PromiseToC1")
	// require.NoError(t, c2PromiseToC1.Resolve(ctx))
	// fmt.Println("Resolved second promise")

	// Shut down the connections. This ensures that we've successfully
	// shortened the path to cut out the remote peer in the next assertion.
	require.NoError(t, c1.Close())
	require.NoError(t, c2.Close())

	// Perform the call using what was originally a C2->C1 promise, but
	// that should have gotten shortened to a local call.
	const c2CallValue int64 = 0xc2000042
	c2Call, c2CallRel := echoNum(ctx, c2PromiseToC1, c2CallValue)
	defer c2CallRel()
	c2CallRes, err := c2Call.Struct()
	require.NoError(t, err)
	require.Equal(t, c2CallValue, c2CallRes.N())
}

// Tests that E-order is respected when fulfilling a promise with something on
// the remote peer.
func TestPromiseOrdering(t *testing.T) {
	t.Parallel()

	t.Cleanup(func() { time.Sleep(100 * time.Millisecond) }) // Avoid log error after fail

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	p, r := capnp.NewLocalPromise[testcapnp.PingPong]()
	defer p.Release()

	left, right := transport.NewPipe(1)
	p1, p2 := rpc.NewTransport(left), rpc.NewTransport(right)

	c1 := rpc.NewConn(p1, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(p),
		RemotePeerID:    rpc.PeerID{Value: "c1"}, // (really, c2)
		AbortTimeout:    15 * time.Second,
	})
	ord := &echoNumOrderChecker{
		t: t,
	}
	c2 := rpc.NewConn(p2, &rpc.Options{
		Logger:          testErrorReporter{tb: t},
		BootstrapClient: capnp.Client(testcapnp.PingPong_ServerToClient(ord)),
		RemotePeerID:    rpc.PeerID{Value: "c2"}, // (really, c1)
		AbortTimeout:    15 * time.Second,
	})

	remotePromise := testcapnp.PingPong(c2.Bootstrap(ctx))
	defer remotePromise.Release()

	time.Sleep(1000 * time.Millisecond)
	fmt.Println("slept to start sending echo calls")
	_ = r

	fulfill := func() {
		time.Sleep(1000 * time.Millisecond)
		go func() {
			// time.Sleep(time.Millisecond * 1000)
			fmt.Println("LLL gonna fulfill")
			r.Fulfill(testcapnp.PingPong(c1.Bootstrap(ctx)))
			fmt.Println("fulfilled")
		}()
		time.Sleep(1000 * time.Millisecond)
	}
	fulfill()

	// Send a whole bunch of calls to the promise:
	var (
		futures []testcapnp.PingPong_echoNum_Results_Future
		rels    []capnp.ReleaseFunc
	)
	numCalls := 16
	for i := 0; i < numCalls; i++ {
		fut, rel := echoNum(ctx, remotePromise, int64(i))
		futures = append(futures, fut)
		rels = append(rels, rel)
		time.Sleep(10 * time.Millisecond)

		// At some arbitrary point in the middle, fulfill the promise
		// with the other bootstrap interface:
		//		if i == numCalls+1 {
		//			go func() {
		//				r.Fulfill(testcapnp.PingPong(c1.Bootstrap(ctx)))
		//			}()
		//		}
	}

	//	time.Sleep(1000 * time.Millisecond)
	//	go func() {
	//		time.Sleep(time.Millisecond * 1000)
	//		fmt.Println("LLL gonna fulfill")
	//		r.Fulfill(testcapnp.PingPong(c1.Bootstrap(ctx)))
	//		fmt.Println("fulfilled")
	//	}()
	// time.Sleep(2000 * time.Millisecond)
	// fulfill()

	for i, fut := range futures {
		// Verify that all the results are as expected. The server
		// Will verify that they came in the right order.
		fmt.Println("XXX gonna ask for ", i)
		start := time.Now()
		res, err := fut.Struct()
		fmt.Printf("YYY got reply %d %d %v\n", i, res.N(), err)
		dur := time.Since(start)
		time.Sleep(10 * time.Millisecond) // <-- some other async issue
		require.NoError(t, err, fmt.Sprintf("call #%d should succeed %v", i, dur))
		require.Equal(t, int64(i), res.N())
	}
	fmt.Println("XXX done all checks")
	time.Sleep(time.Second)
	for _, rel := range rels {
		rel()
	}
	fmt.Println("XXX released all futures")
	time.Sleep(time.Second)

	require.NoError(t, remotePromise.Resolve(ctx))
	// Shut down the connections, and make sure we can still send
	// calls. This ensures that we've successfully shortened the path to
	// cut out the remote peer:
	c1.Close()
	c2.Close()
	fut, rel := echoNum(ctx, remotePromise, int64(numCalls))
	defer rel()
	res, err := fut.Struct()
	require.NoError(t, err)
	require.Equal(t, int64(numCalls), res.N())
}
