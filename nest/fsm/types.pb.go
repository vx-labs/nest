// Code generated by protoc-gen-go. DO NOT EDIT.
// source: types.proto

package fsm

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type PeerLost struct {
	Peer                 uint64   `protobuf:"varint,1,opt,name=Peer,proto3" json:"Peer,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PeerLost) Reset()         { *m = PeerLost{} }
func (m *PeerLost) String() string { return proto.CompactTextString(m) }
func (*PeerLost) ProtoMessage()    {}
func (*PeerLost) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{0}
}

func (m *PeerLost) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PeerLost.Unmarshal(m, b)
}
func (m *PeerLost) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PeerLost.Marshal(b, m, deterministic)
}
func (m *PeerLost) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PeerLost.Merge(m, src)
}
func (m *PeerLost) XXX_Size() int {
	return xxx_messageInfo_PeerLost.Size(m)
}
func (m *PeerLost) XXX_DiscardUnknown() {
	xxx_messageInfo_PeerLost.DiscardUnknown(m)
}

var xxx_messageInfo_PeerLost proto.InternalMessageInfo

func (m *PeerLost) GetPeer() uint64 {
	if m != nil {
		return m.Peer
	}
	return 0
}

type StateTransitionSet struct {
	Events               []*StateTransition `protobuf:"bytes,1,rep,name=events,proto3" json:"events,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *StateTransitionSet) Reset()         { *m = StateTransitionSet{} }
func (m *StateTransitionSet) String() string { return proto.CompactTextString(m) }
func (*StateTransitionSet) ProtoMessage()    {}
func (*StateTransitionSet) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{1}
}

func (m *StateTransitionSet) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StateTransitionSet.Unmarshal(m, b)
}
func (m *StateTransitionSet) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StateTransitionSet.Marshal(b, m, deterministic)
}
func (m *StateTransitionSet) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StateTransitionSet.Merge(m, src)
}
func (m *StateTransitionSet) XXX_Size() int {
	return xxx_messageInfo_StateTransitionSet.Size(m)
}
func (m *StateTransitionSet) XXX_DiscardUnknown() {
	xxx_messageInfo_StateTransitionSet.DiscardUnknown(m)
}

var xxx_messageInfo_StateTransitionSet proto.InternalMessageInfo

func (m *StateTransitionSet) GetEvents() []*StateTransition {
	if m != nil {
		return m.Events
	}
	return nil
}

type RecordsPut struct {
	Records              [][]byte `protobuf:"bytes,1,rep,name=Records,proto3" json:"Records,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RecordsPut) Reset()         { *m = RecordsPut{} }
func (m *RecordsPut) String() string { return proto.CompactTextString(m) }
func (*RecordsPut) ProtoMessage()    {}
func (*RecordsPut) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{2}
}

func (m *RecordsPut) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RecordsPut.Unmarshal(m, b)
}
func (m *RecordsPut) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RecordsPut.Marshal(b, m, deterministic)
}
func (m *RecordsPut) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RecordsPut.Merge(m, src)
}
func (m *RecordsPut) XXX_Size() int {
	return xxx_messageInfo_RecordsPut.Size(m)
}
func (m *RecordsPut) XXX_DiscardUnknown() {
	xxx_messageInfo_RecordsPut.DiscardUnknown(m)
}

var xxx_messageInfo_RecordsPut proto.InternalMessageInfo

func (m *RecordsPut) GetRecords() [][]byte {
	if m != nil {
		return m.Records
	}
	return nil
}

type StateTransition struct {
	// Types that are valid to be assigned to Event:
	//	*StateTransition_PeerLost
	//	*StateTransition_RecordsPut
	Event                isStateTransition_Event `protobuf_oneof:"Event"`
	XXX_NoUnkeyedLiteral struct{}                `json:"-"`
	XXX_unrecognized     []byte                  `json:"-"`
	XXX_sizecache        int32                   `json:"-"`
}

func (m *StateTransition) Reset()         { *m = StateTransition{} }
func (m *StateTransition) String() string { return proto.CompactTextString(m) }
func (*StateTransition) ProtoMessage()    {}
func (*StateTransition) Descriptor() ([]byte, []int) {
	return fileDescriptor_d938547f84707355, []int{3}
}

func (m *StateTransition) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StateTransition.Unmarshal(m, b)
}
func (m *StateTransition) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StateTransition.Marshal(b, m, deterministic)
}
func (m *StateTransition) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StateTransition.Merge(m, src)
}
func (m *StateTransition) XXX_Size() int {
	return xxx_messageInfo_StateTransition.Size(m)
}
func (m *StateTransition) XXX_DiscardUnknown() {
	xxx_messageInfo_StateTransition.DiscardUnknown(m)
}

var xxx_messageInfo_StateTransition proto.InternalMessageInfo

type isStateTransition_Event interface {
	isStateTransition_Event()
}

type StateTransition_PeerLost struct {
	PeerLost *PeerLost `protobuf:"bytes,1,opt,name=PeerLost,proto3,oneof"`
}

type StateTransition_RecordsPut struct {
	RecordsPut *RecordsPut `protobuf:"bytes,2,opt,name=RecordsPut,proto3,oneof"`
}

func (*StateTransition_PeerLost) isStateTransition_Event() {}

func (*StateTransition_RecordsPut) isStateTransition_Event() {}

func (m *StateTransition) GetEvent() isStateTransition_Event {
	if m != nil {
		return m.Event
	}
	return nil
}

func (m *StateTransition) GetPeerLost() *PeerLost {
	if x, ok := m.GetEvent().(*StateTransition_PeerLost); ok {
		return x.PeerLost
	}
	return nil
}

func (m *StateTransition) GetRecordsPut() *RecordsPut {
	if x, ok := m.GetEvent().(*StateTransition_RecordsPut); ok {
		return x.RecordsPut
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*StateTransition) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*StateTransition_PeerLost)(nil),
		(*StateTransition_RecordsPut)(nil),
	}
}

func init() {
	proto.RegisterType((*PeerLost)(nil), "fsm.PeerLost")
	proto.RegisterType((*StateTransitionSet)(nil), "fsm.StateTransitionSet")
	proto.RegisterType((*RecordsPut)(nil), "fsm.RecordsPut")
	proto.RegisterType((*StateTransition)(nil), "fsm.StateTransition")
}

func init() { proto.RegisterFile("types.proto", fileDescriptor_d938547f84707355) }

var fileDescriptor_d938547f84707355 = []byte{
	// 199 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2e, 0xa9, 0x2c, 0x48,
	0x2d, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x4e, 0x2b, 0xce, 0x55, 0x92, 0xe3, 0xe2,
	0x08, 0x48, 0x4d, 0x2d, 0xf2, 0xc9, 0x2f, 0x2e, 0x11, 0x12, 0xe2, 0x62, 0x01, 0xb1, 0x25, 0x18,
	0x15, 0x18, 0x35, 0x58, 0x82, 0xc0, 0x6c, 0x25, 0x27, 0x2e, 0xa1, 0xe0, 0x92, 0xc4, 0x92, 0xd4,
	0x90, 0xa2, 0xc4, 0xbc, 0xe2, 0xcc, 0x92, 0xcc, 0xfc, 0xbc, 0xe0, 0xd4, 0x12, 0x21, 0x1d, 0x2e,
	0xb6, 0xd4, 0xb2, 0xd4, 0xbc, 0x92, 0x62, 0x09, 0x46, 0x05, 0x66, 0x0d, 0x6e, 0x23, 0x11, 0xbd,
	0xb4, 0xe2, 0x5c, 0x3d, 0x34, 0x85, 0x41, 0x50, 0x35, 0x4a, 0x6a, 0x5c, 0x5c, 0x41, 0xa9, 0xc9,
	0xf9, 0x45, 0x29, 0xc5, 0x01, 0xa5, 0x25, 0x42, 0x12, 0x5c, 0xec, 0x50, 0x1e, 0x58, 0x33, 0x4f,
	0x10, 0x8c, 0xab, 0x54, 0xc5, 0xc5, 0x8f, 0x66, 0x84, 0x90, 0x36, 0xc2, 0x79, 0x60, 0x67, 0x71,
	0x1b, 0xf1, 0x82, 0xad, 0x82, 0x09, 0x7a, 0x30, 0x04, 0x21, 0xdc, 0x6f, 0x88, 0x6c, 0x8f, 0x04,
	0x13, 0x58, 0x39, 0x3f, 0x58, 0x39, 0x42, 0xd8, 0x83, 0x21, 0x08, 0x49, 0x91, 0x13, 0x3b, 0x17,
	0xab, 0x2b, 0xc8, 0x91, 0x49, 0x6c, 0xe0, 0x30, 0x31, 0x06, 0x04, 0x00, 0x00, 0xff, 0xff, 0x8d,
	0xdc, 0x3d, 0x98, 0x22, 0x01, 0x00, 0x00,
}
