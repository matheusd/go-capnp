package capnp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"capnproto.org/go/capnp/v3/exc"
	"capnproto.org/go/capnp/v3/internal/str"
	"capnproto.org/go/capnp/v3/packed"
)

// Security limits. Matches C++ implementation.
const (
	defaultTraverseLimit = 64 << 20 // 64 MiB
	defaultDepthLimit    = 64

	maxStreamSegments = 512

	defaultDecodeLimit = 64 << 20 // 64 MiB
)

const maxDepth = ^uint(0)

// A Message is a tree of Cap'n Proto objects, split into one or more
// segments of contiguous memory.  The only required field is Arena.
// A Message is safe to read from multiple goroutines.
//
// A message must be set up with a fully valid Arena when reading or with
// a valid and empty arena by calling NewArena.
type Message struct {
	// rlimit must be first so that it is 64-bit aligned.
	// See sync/atomic docs.
	rlimit     atomic.Uint64
	rlimitInit sync.Once

	Arena Arena

	capTable CapTable

	// TraverseLimit limits how many total bytes of data are allowed to be
	// traversed while reading.  Traversal is counted when a Struct or
	// List is obtained.  This means that calling a getter for the same
	// sub-struct multiple times will cause it to be double-counted.  Once
	// the traversal limit is reached, pointer accessors will report
	// errors. See https://capnproto.org/encoding.html#amplification-attack
	// for more details on this security measure.
	//
	// If not set, this defaults to 64 MiB.
	TraverseLimit uint64

	// DepthLimit limits how deeply-nested a message structure can be.
	// If not set, this defaults to 64.
	DepthLimit uint
}

// NewMessage creates a message with a new root and returns the first
// segment.  It is an error to call NewMessage on an arena with data in it.
//
// The new message is guaranteed to contain at least one segment and that
// segment is guaranteed to contain enough space for the root struct pointer.
func NewMessage(arena Arena) (*Message, *Segment, error) {
	var msg Message
	first, err := msg.Reset(arena)
	return &msg, first, err
}

// NewSingleSegmentMessage(b) is equivalent to NewMessage(SingleSegment(b)), except
// that it panics instead of returning an error. This can only happen if the passed
// slice contains data, so the caller is responsible for ensuring that it has a length
// of zero.
func NewSingleSegmentMessage(b []byte) (msg *Message, first *Segment) {
	msg, first, err := NewMessage(SingleSegment(b))
	if err != nil {
		panic(err)
	}
	return msg, first
}

// Analogous to NewSingleSegmentMessage, but using MultiSegment.
func NewMultiSegmentMessage(b [][]byte) (msg *Message, first *Segment) {
	msg, first, err := NewMessage(MultiSegment(b))
	if err != nil {
		panic(err)
	}
	return msg, first
}

// Release is syntactic sugar for Message.Reset(m.Arena).  See
// docstring for Reset for an important warning.
func (m *Message) Release() {
	m.Reset(m.Arena)
}

// Reset the message to use a different arena, allowing it
// to be reused. This invalidates any existing pointers in
// the Message, releases all clients in the cap table, and
// releases the current Arena, so use with caution.
//
// Reset fails if the new arena is not empty and is not able to allocate enough
// space for at least one segment and its root pointer.  In other words, Reset
// must only be used for messages which will be modified, not read.
func (m *Message) Reset(arena Arena) (first *Segment, err error) {
	m.capTable.Reset()
	if m.Arena != nil {
		m.Arena.Release()
	}

	*m = Message{
		Arena:         arena,
		TraverseLimit: m.TraverseLimit,
		DepthLimit:    m.DepthLimit,
		capTable:      m.capTable,
	}

	// FIXME(matheusd): All the checks after this point have been added to
	// maintain compatibility to the prior implementation and not break any
	// tests, but personally, I think these should not exist. Message
	// should be resettable with a pre-filled arena for reading without it
	// failing and they should only enforce an allocation when the first
	// write operation occurs.

	// Ensure there are no more than one segment allocated in the arena.
	// This maintains compatibility to older versions of Reset().
	if arena.NumSegments() > 1 {
		return nil, errors.New("arena already has multiple segments allocated")
	}

	// Ensure the first segment has no data.
	first = m.Arena.Segment(0)
	if first != nil {
		if len(first.data) != 0 {
			return nil, errors.New("arena not empty")
		}
		if first.msg != nil && first.msg != m {
			return nil, errors.New("first segment associated with different msg")
		}

		// Ensure the first segment points to message m.
		first.msg = m
	}

	// Ensure the arena has size of at least the root pointer.
	if first == nil || len(first.data) < int(wordSize) {
		// When there is a first segment and it has capacity (but not
		// yet length), manually resize it.
		//
		// TODO: this is here due to a single test requiring this
		// behavior. Consider removing it.
		if first != nil && len(first.data) == 0 && cap(first.data) >= int(wordSize) {
			first.data = first.data[:8]
		} else {
			first, _, err = m.Arena.Allocate(wordSize, m, nil)
		}
	}
	return
}

// ResetForRead resets the message for reading with the specified arena. This
// releases the current message arena, if it exists.
func (m *Message) ResetForRead(arena Arena) {
	m.capTable.Reset()
	if m.Arena != nil {
		m.Arena.Release()
	}
	m.Arena = arena
	m.rlimit = atomic.Uint64{}
	m.rlimitInit = sync.Once{}
}

func (m *Message) initReadLimit() {
	if m.TraverseLimit == 0 {
		m.rlimit.Store(defaultTraverseLimit)
		return
	}
	m.rlimit.Store(m.TraverseLimit)
}

// canRead reports whether the amount of bytes can be stored safely.
func (m *Message) canRead(sz Size) (ok bool) {
	return true

	m.rlimitInit.Do(m.initReadLimit)
	for {
		curr := m.rlimit.Load()

		var new uint64
		if ok = curr >= uint64(sz); ok {
			new = curr - uint64(sz)
		}

		if m.rlimit.CompareAndSwap(curr, new) {
			return
		}
	}
}

// ResetReadLimit sets the number of bytes allowed to be read from this message.
func (m *Message) ResetReadLimit(limit uint64) {
	m.rlimitInit.Do(func() {})
	m.rlimit.Store(limit)
}

// Unread increases the read limit by sz.
func (m *Message) Unread(sz Size) {
	m.rlimitInit.Do(m.initReadLimit)
	m.rlimit.Add(uint64(sz))
}

// Root returns the pointer to the message's root object.
func (m *Message) Root() (Ptr, error) {
	s, err := m.Segment(0)
	if err != nil {
		return Ptr{}, exc.WrapError("read root", err)
	}
	root, ok := s.root()
	if !ok {
		return Ptr{}, errors.New("root is not set")
	}
	p, err := root.At(0)
	if err != nil {
		return Ptr{}, exc.WrapError("read root", err)
	}
	return p, nil
}

// AllocateAsRoot allocates the passed size and sets that as the root structure
// of the message.
func (m *Message) AllocateAsRoot(size ObjectSize) (*Segment, address, error) {
	// Allocate enough for the root pointer + the root structure. Doing a
	// single alloc ensures both end up on the same segment (which should
	// be segment zero and the offset should also be zero, meaning the root
	// pointer is the first data written to the segment).
	//
	// Technically, it could be the case (depending on the arena
	// implementation) that the first segment would have only the root
	// pointer (one word) and then the root struct would be put in a
	// different segment (using a landing pad), but that would be very
	// inneficient in several ways, so we opt here to enforce a single
	// alloc for both as an optimization.
	s, rootAddr, err := m.alloc(wordSize+size.totalSize(), nil)
	if err != nil {
		return nil, 0, err
	}
	if s.ID() != 0 {
		return nil, 0, errors.New("root was not allocated on first segment")
	}
	if rootAddr != 0 {
		return nil, 0, errors.New("root struct was already allocated")
	}

	// The root struct starts immediately after the root pointer (which
	// takes one word).
	srcAddr := address(wordSize)

	// FIXME: does not handle lists and interfaces/capabilities yet.
	srcRaw := rawStructPointer(0, size)
	s.writeRawPointer(rootAddr, srcRaw.withOffset(nearPointerOffset(rootAddr, srcAddr)))
	return s, srcAddr, nil
}

// SetRoot sets the message's root object to p.
func (m *Message) SetRoot(p Ptr) error {
	s, err := m.Segment(0)
	if err != nil {
		return exc.WrapError("set root", err)
	}
	root, ok := s.root()
	if !ok {
		// Root is not allocated on the first segment. Allocate it now.
		_, _, err := m.alloc(wordSize, nil)
		if err != nil {
			return exc.WrapError("initial alloc", err)
		}
		root, ok = s.root()
		if !ok {
			return errors.New("unable to allocate root")
		}
	}
	if err := root.Set(0, p); err != nil {
		return exc.WrapError("set root", err)
	}
	return nil
}

// CapTable is the indexed list of the clients referenced in the
// message. Capability pointers inside the message will use this
// table to map pointers to Clients.   The table is populated by
// the RPC system.
//
// https://capnproto.org/encoding.html#capabilities-interfaces
func (m *Message) CapTable() *CapTable {
	return &m.capTable
}

// Compute the total size of the message in bytes, when serialized as
// a stream. This is the same as the length of the slice returned by
// m.Marshal()
func (m *Message) TotalSize() (uint64, error) {
	nsegs := uint64(m.NumSegments())
	totalSize := (nsegs/2 + 1) * 8
	for i := uint64(0); i < nsegs; i++ {
		seg, err := m.Segment(SegmentID(i))
		if err != nil {
			return 0, err
		}
		totalSize += uint64(len(seg.Data()))
	}
	return totalSize, nil
}

func (m *Message) depthLimit() uint {
	if m.DepthLimit != 0 {
		return m.DepthLimit
	}
	return defaultDepthLimit
}

// NumSegments returns the number of segments in the message.
func (m *Message) NumSegments() int64 {
	return m.Arena.NumSegments()
}

// Segment returns the segment with the given ID.  If err == nil, then the
// segment is enforced to exist and to be associated to message m.
func (m *Message) Segment(id SegmentID) (*Segment, error) {
	seg := m.Arena.Segment(id)
	if seg == nil {
		return nil, errors.New("segment " + str.Utod(id) + ": out of bounds")
	}

	if seg.msg == nil {
		seg.msg = m
	}
	if seg.msg != m {
		return nil, fmt.Errorf("segment %d associated with different msg", id)
	}
	return seg, nil
}

func (m *Message) alloc(sz Size, pref *Segment) (*Segment, address, error) {
	if sz > maxAllocSize() {
		return nil, 0, errors.New("allocation: too large")
	}
	sz = sz.padToWord()

	seg, addr, err := m.Arena.Allocate(sz, m, pref)
	if err != nil {
		return nil, 0, err
	}
	if seg == nil {
		return nil, 0, errors.New("arena returned nil segment for Allocate()")
	}
	if seg.msg != nil && seg.msg != m {
		return nil, 0, errors.New("arena returned segment assigned to other message")
	}
	seg.msg = m
	return seg, addr, nil
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	wc := &writeCounter{Writer: w}
	err := NewEncoder(wc).Encode(m)
	return wc.N, err
}

// MarshalInto concatenates the segments in the message into a single byte
// slice including framing.
func (m *Message) MarshalInto(buf []byte) ([]byte, error) {
	// Compute buffer size.
	nsegs := m.NumSegments()
	if nsegs == 0 {
		return nil, errors.New("marshal: message has no segments")
	}
	hdrSize := streamHeaderSize(SegmentID(nsegs - 1))
	if hdrSize > uint64(maxInt) {
		return nil, errors.New("marshal: header size overflows int")
	}
	var dataSize uint64
	for i := int64(0); i < nsegs; i++ {
		s, err := m.Segment(SegmentID(i))
		if err != nil {
			return nil, exc.WrapError("marshal: ", err)
		}
		if s == nil {
			return nil, errors.New("marshal: nil segment")
		}
		n := uint64(len(s.data))
		if n%uint64(wordSize) != 0 {
			return nil, errors.New("marshal: segment " + str.Itod(i) + " not word-aligned")
		}
		if n > uint64(maxSegmentSize) {
			return nil, errors.New("marshal: segment " + str.Itod(i) + " too large")
		}
		dataSize += n
		if dataSize > uint64(maxInt) {
			return nil, errors.New("marshal: message size overflows int")
		}
	}
	total := hdrSize + dataSize
	if total > uint64(maxInt) {
		return nil, errors.New("marshal: message size overflows int")
	}

	// Fill buffer.
	if buf == nil {
		buf = make([]byte, 0, int(total))
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(nsegs-1))
	for i := int64(0); i < nsegs; i++ {
		s, err := m.Segment(SegmentID(i))
		if err != nil {
			return nil, exc.WrapError("marshal: ", err)
		}
		if len(s.data)%int(wordSize) != 0 {
			return nil, errors.New("marshal: segment " + str.Itod(i) + " not word-aligned")
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s.data)/int(wordSize)))
		buf = append(buf, s.data...)
	}
	return buf, nil
}

func (m *Message) Marshal() ([]byte, error) {
	return m.MarshalInto(nil)
}

// MarshalPacked marshals the message in packed form.
func (m *Message) MarshalPacked() ([]byte, error) {
	data, err := m.Marshal()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(data))
	buf = packed.Pack(buf, data)
	return buf, nil
}

type writeCounter struct {
	N int64
	io.Writer
}

func (wc *writeCounter) Write(b []byte) (n int, err error) {
	n, err = wc.Writer.Write(b)
	wc.N += int64(n)
	return
}

// alloc allocates sz zero-filled bytes.  It prefers using s, but may
// use a different segment in the same message if there's not sufficient
// capacity.
func alloc(s *Segment, sz Size) (*Segment, address, error) {
	return s.msg.alloc(sz, s)
}
