// Code generated by protoc-gen-go. DO NOT EDIT.
// source: key_value.proto

package kvstore

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

// A collection of key-value pairs
type KeyValueStore struct {
	Pairs                []*KeyValue `protobuf:"bytes,1,rep,name=pairs,proto3" json:"pairs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *KeyValueStore) Reset()         { *m = KeyValueStore{} }
func (m *KeyValueStore) String() string { return proto.CompactTextString(m) }
func (*KeyValueStore) ProtoMessage()    {}
func (*KeyValueStore) Descriptor() ([]byte, []int) {
	return fileDescriptor_40f3a6d8264e424e, []int{0}
}

func (m *KeyValueStore) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyValueStore.Unmarshal(m, b)
}
func (m *KeyValueStore) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyValueStore.Marshal(b, m, deterministic)
}
func (m *KeyValueStore) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyValueStore.Merge(m, src)
}
func (m *KeyValueStore) XXX_Size() int {
	return xxx_messageInfo_KeyValueStore.Size(m)
}
func (m *KeyValueStore) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyValueStore.DiscardUnknown(m)
}

var xxx_messageInfo_KeyValueStore proto.InternalMessageInfo

func (m *KeyValueStore) GetPairs() []*KeyValue {
	if m != nil {
		return m.Pairs
	}
	return nil
}

// A single key-value pair
type KeyValue struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value                string   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyValue) Reset()         { *m = KeyValue{} }
func (m *KeyValue) String() string { return proto.CompactTextString(m) }
func (*KeyValue) ProtoMessage()    {}
func (*KeyValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_40f3a6d8264e424e, []int{1}
}

func (m *KeyValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyValue.Unmarshal(m, b)
}
func (m *KeyValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyValue.Marshal(b, m, deterministic)
}
func (m *KeyValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyValue.Merge(m, src)
}
func (m *KeyValue) XXX_Size() int {
	return xxx_messageInfo_KeyValue.Size(m)
}
func (m *KeyValue) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyValue.DiscardUnknown(m)
}

var xxx_messageInfo_KeyValue proto.InternalMessageInfo

func (m *KeyValue) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyValue) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterType((*KeyValueStore)(nil), "kvstore.KeyValueStore")
	proto.RegisterType((*KeyValue)(nil), "kvstore.KeyValue")
}

func init() {
	proto.RegisterFile("key_value.proto", fileDescriptor_40f3a6d8264e424e)
}

var fileDescriptor_40f3a6d8264e424e = []byte{
	// 127 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0xcf, 0x4e, 0xad, 0x8c,
	0x2f, 0x4b, 0xcc, 0x29, 0x4d, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0xcf, 0x2e, 0x2b,
	0x2e, 0xc9, 0x2f, 0x4a, 0x55, 0xb2, 0xe0, 0xe2, 0xf5, 0x4e, 0xad, 0x0c, 0x03, 0x49, 0x05, 0x83,
	0x04, 0x84, 0xd4, 0xb9, 0x58, 0x0b, 0x12, 0x33, 0x8b, 0x8a, 0x25, 0x18, 0x15, 0x98, 0x35, 0xb8,
	0x8d, 0x04, 0xf5, 0xa0, 0x2a, 0xf5, 0x60, 0xca, 0x82, 0x20, 0xf2, 0x4a, 0x46, 0x5c, 0x1c, 0x30,
	0x21, 0x21, 0x01, 0x2e, 0xe6, 0xec, 0xd4, 0x4a, 0x09, 0x46, 0x05, 0x46, 0x0d, 0xce, 0x20, 0x10,
	0x53, 0x48, 0x84, 0x8b, 0x15, 0x6c, 0x9f, 0x04, 0x13, 0x58, 0x0c, 0xc2, 0x49, 0x62, 0x03, 0xdb,
	0x6e, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0x4d, 0x68, 0x15, 0x3f, 0x90, 0x00, 0x00, 0x00,
}
